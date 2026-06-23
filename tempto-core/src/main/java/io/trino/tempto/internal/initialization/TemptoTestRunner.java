/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.tempto.internal.initialization;

import com.google.inject.Module;
import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.Requirement;
import io.trino.tempto.context.TestContext;
import io.trino.tempto.fulfillment.TestStatus;
import io.trino.tempto.initialization.TestMethodInfo;
import io.trino.tempto.internal.ReflectionInjectorHelper;
import io.trino.tempto.internal.TestSpecificRequirementsResolver;
import io.trino.tempto.internal.context.GuiceTestContext;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.tempto.context.ThreadLocalTestContextHolder.popAllTestContexts;
import static io.trino.tempto.context.ThreadLocalTestContextHolder.pushAllTestContexts;
import static io.trino.tempto.context.ThreadLocalTestContextHolder.restoreTestContexts;
import static io.trino.tempto.context.ThreadLocalTestContextHolder.saveAndClearTestContexts;
import static io.trino.tempto.context.ThreadLocalTestContextHolder.testContext;
import static io.trino.tempto.context.ThreadLocalTestContextHolder.testContextIfSet;
import static io.trino.tempto.fulfillment.TestStatus.FAILURE;
import static io.trino.tempto.internal.logging.LoggingMdcHelper.cleanLoggingMdc;
import static io.trino.tempto.internal.logging.LoggingMdcHelper.setupLoggingMdcForTest;
import static java.util.Collections.emptyList;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;

/**
 * Per-test lifecycle: builds a child {@link TestContext} on top of the suite context, fulfills the
 * test-method level requirements, injects the test instance, invokes {@link BeforeMethodWithContext}
 * /{@link AfterMethodWithContext} methods and cleans up afterwards.
 * <p>
 * This is shared by {@link TemptoTestExtension} (regular product tests) and by the convention-based
 * tests which run as JUnit dynamic tests.
 */
public class TemptoTestRunner
{
    private final TemptoRuntime runtime;
    private final ReflectionInjectorHelper reflectionInjectorHelper = new ReflectionInjectorHelper();
    private final TestSpecificRequirementsResolver requirementsResolver;

    public TemptoTestRunner(TemptoRuntime runtime)
    {
        this.runtime = requireNonNull(runtime, "runtime is null");
        this.requirementsResolver = new TestSpecificRequirementsResolver(runtime.getConfiguration());
    }

    /**
     * Sets up the per-test context and runs {@link BeforeMethodWithContext} methods. Must be paired
     * with {@link #tearDown}.
     */
    public RunningTest setUp(TestContext suiteContext, TestMethodInfo testMethodInfo)
    {
        setupLoggingMdcForTest(testMethodInfo.testName());

        // Under JUnit's parallel (work-stealing ForkJoinPool) executor a worker thread can begin a
        // test while it is already in the middle of another one (during a join). Detach any context
        // currently bound to this thread and restore it in tearDown so nested tests do not clobber
        // each other's thread-local context.
        Optional<Deque<TestContext>> previousContexts = saveAndClearTestContexts();
        try {
            GuiceTestContext initTestContext = ((GuiceTestContext) suiteContext).createChildContext(emptyList(), getTestModules(testMethodInfo));
            Deque<TestContext> testContextStack = new ArrayDeque<>();
            testContextStack.addLast(initTestContext);

            Set<Requirement> testSpecificRequirements = resolveTestSpecificRequirements(testMethodInfo);
            Fulfillment.fulfill(testContextStack, runtime.getTestMethodLevelFulfillers(), testSpecificRequirements);

            pushAllTestContexts(testContextStack);
            TestContext topTestContext = testContextStack.getLast();
            topTestContext.injectMembers(testMethodInfo.testInstance());

            invokeMethodsAnnotatedWith(BeforeMethodWithContext.class, testMethodInfo, topTestContext, ClassOrdering.SUPER_FIRST);

            return new RunningTest(testMethodInfo, previousContexts);
        }
        catch (RuntimeException e) {
            if (testContextIfSet().isPresent()) {
                Deque<TestContext> stack = popAllTestContexts();
                Fulfillment.cleanup(stack, runtime.getTestMethodLevelFulfillers(), FAILURE);
            }
            restoreTestContexts(previousContexts);
            cleanLoggingMdc();
            throw e;
        }
    }

    /**
     * Runs {@link AfterMethodWithContext} methods and tears down the per-test context.
     */
    public void tearDown(RunningTest runningTest, TestStatus testStatus)
    {
        try {
            if (!testContextIfSet().isPresent()) {
                return;
            }

            boolean runAfterSucceeded = false;
            try {
                invokeMethodsAnnotatedWith(AfterMethodWithContext.class, runningTest.testMethodInfo(), testContext(), ClassOrdering.SUPER_LAST);
                runAfterSucceeded = true;
            }
            finally {
                Deque<TestContext> testContextStack = popAllTestContexts();
                Fulfillment.cleanup(testContextStack, runtime.getTestMethodLevelFulfillers(), runAfterSucceeded ? testStatus : FAILURE);
            }
        }
        finally {
            restoreTestContexts(runningTest.previousContexts());
            cleanLoggingMdc();
        }
    }

    private Set<Requirement> resolveTestSpecificRequirements(TestMethodInfo testMethodInfo)
    {
        Method method = testMethodInfo.testMethod()
                .orElseThrow(() -> new IllegalStateException("test method is required to resolve requirements for " + testMethodInfo.testName()));
        Set<Set<Requirement>> requirementsSets = requirementsResolver.resolve(method, Optional.of(testMethodInfo.testInstance()));
        if (requirementsSets.size() != 1) {
            throw new UnsupportedOperationException(
                    "Test %s resolves to %d requirement sets. Expanding a single test into multiple instances via Requirements.allOf() "
                            .formatted(testMethodInfo.testName(), requirementsSets.size())
                            + "is not supported on JUnit; use a parameterized test (@ParameterizedTest) instead.");
        }
        return getOnlyElement(requirementsSets);
    }

    private List<Module> getTestModules(TestMethodInfo testMethodInfo)
    {
        return runtime.getTestMethodModuleProviders()
                .stream()
                .map(provider -> provider.getModule(runtime.getConfiguration(), testMethodInfo))
                .collect(toImmutableList());
    }

    private void invokeMethodsAnnotatedWith(Class<? extends Annotation> annotationClass, TestMethodInfo testMethodInfo, TestContext testContext, ClassOrdering ordering)
    {
        Comparator<Class<?>> depthComparator = comparing(clazz -> {
            int depth = 0;
            while (clazz.getSuperclass() != null) {
                clazz = clazz.getSuperclass();
                depth++;
            }
            return depth;
        });
        Comparator<Class<?>> classComparator = switch (ordering) {
            case SUPER_FIRST -> depthComparator;
            case SUPER_LAST -> depthComparator.reversed();
        };

        Stream.of(testMethodInfo.testClass().getMethods())
                .filter(declaredMethod -> declaredMethod.isAnnotationPresent(annotationClass))
                .sorted(comparing(Method::getDeclaringClass, classComparator))
                .forEachOrdered(declaredMethod -> {
                    try {
                        declaredMethod.invoke(testMethodInfo.testInstance(), reflectionInjectorHelper.getMethodArguments(testContext, declaredMethod));
                    }
                    catch (IllegalAccessException | InvocationTargetException e) {
                        throw new RuntimeException("error invoking method annotated with " + annotationClass.getName(), e);
                    }
                });
    }

    public record RunningTest(TestMethodInfo testMethodInfo, Optional<Deque<TestContext>> previousContexts) {}

    private enum ClassOrdering
    {
        SUPER_FIRST,
        SUPER_LAST,
    }
}
