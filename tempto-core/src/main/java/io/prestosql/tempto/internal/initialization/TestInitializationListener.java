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

package io.prestosql.tempto.internal.initialization;

import com.beust.jcommander.internal.Sets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Singleton;
import io.prestosql.tempto.AfterTestWithContext;
import io.prestosql.tempto.BeforeTestWithContext;
import io.prestosql.tempto.Requirement;
import io.prestosql.tempto.TemptoPlugin;
import io.prestosql.tempto.configuration.Configuration;
import io.prestosql.tempto.context.TestContext;
import io.prestosql.tempto.fulfillment.RequirementFulfiller;
import io.prestosql.tempto.fulfillment.RequirementFulfiller.SuiteLevelFulfiller;
import io.prestosql.tempto.fulfillment.RequirementFulfiller.TestLevelFulfiller;
import io.prestosql.tempto.fulfillment.TestStatus;
import io.prestosql.tempto.fulfillment.table.TableManager;
import io.prestosql.tempto.fulfillment.table.TableManagerDispatcher;
import io.prestosql.tempto.initialization.SuiteModuleProvider;
import io.prestosql.tempto.initialization.TestMethodModuleProvider;
import io.prestosql.tempto.internal.ReflectionHelper;
import io.prestosql.tempto.internal.ReflectionInjectorHelper;
import io.prestosql.tempto.internal.TestSpecificRequirementsResolver;
import io.prestosql.tempto.internal.context.GuiceTestContext;
import io.prestosql.tempto.internal.context.TestContextStack;
import org.slf4j.Logger;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestNGMethod;
import org.testng.ITestResult;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.beust.jcommander.internal.Lists.newArrayList;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Lists.reverse;
import static com.google.inject.util.Modules.combine;
import static io.prestosql.tempto.context.TestContextDsl.runWithTestContext;
import static io.prestosql.tempto.context.ThreadLocalTestContextHolder.assertTestContextNotSet;
import static io.prestosql.tempto.context.ThreadLocalTestContextHolder.popAllTestContexts;
import static io.prestosql.tempto.context.ThreadLocalTestContextHolder.pushAllTestContexts;
import static io.prestosql.tempto.context.ThreadLocalTestContextHolder.testContext;
import static io.prestosql.tempto.context.ThreadLocalTestContextHolder.testContextIfSet;
import static io.prestosql.tempto.fulfillment.TestStatus.FAILURE;
import static io.prestosql.tempto.fulfillment.TestStatus.SUCCESS;
import static io.prestosql.tempto.internal.configuration.TestConfigurationFactory.testConfiguration;
import static io.prestosql.tempto.internal.logging.LoggingMdcHelper.cleanLoggingMdc;
import static io.prestosql.tempto.internal.logging.LoggingMdcHelper.setupLoggingMdcForTest;
import static java.util.Collections.emptyList;
import static org.slf4j.LoggerFactory.getLogger;

public class TestInitializationListener
        implements ITestListener
{
    private static final Logger LOGGER = getLogger(TestInitializationListener.class);

    private final List<? extends SuiteModuleProvider> suiteModuleProviders;
    private final List<? extends TestMethodModuleProvider> testMethodModuleProviders;
    private final List<Class<? extends RequirementFulfiller>> suiteLevelFulfillers;
    private final List<Class<? extends RequirementFulfiller>> testMethodLevelFulfillers;
    private final ReflectionInjectorHelper reflectionInjectorHelper = new ReflectionInjectorHelper();

    private final Configuration configuration;
    private Optional<TestContextStack<TestContext>> suiteTestContextStack = Optional.empty();

    public TestInitializationListener()
    {
        this(ImmutableList.copyOf(ServiceLoader.load(TemptoPlugin.class).iterator()));
    }

    private TestInitializationListener(List<TemptoPlugin> plugins)
    {
        this(
                plugins.stream()
                        .flatMap(plugin -> plugin.getSuiteModules().stream())
                        .map(ReflectionHelper::instantiate)
                        .collect(toImmutableList()),
                plugins.stream()
                        .flatMap(plugin -> plugin.getTestModules().stream())
                        .map(ReflectionHelper::instantiate)
                        .collect(toImmutableList()),
                plugins.stream()
                        .flatMap(plugin -> plugin.getFulfillers().stream())
                        .filter(clazz -> clazz.isAnnotationPresent(SuiteLevelFulfiller.class))
                        .collect(toImmutableList()),
                plugins.stream()
                        .flatMap(plugin -> plugin.getFulfillers().stream())
                        .filter(clazz -> clazz.isAnnotationPresent(TestLevelFulfiller.class))
                        .collect(toImmutableList()),
                testConfiguration());
    }

    TestInitializationListener(
            List<? extends SuiteModuleProvider> suiteModuleProviders,
            List<? extends TestMethodModuleProvider> testMethodModuleProviders,
            List<Class<? extends RequirementFulfiller>> suiteLevelFulfillers,
            List<Class<? extends RequirementFulfiller>> testMethodLevelFulfillers,
            Configuration configuration)
    {
        this.suiteModuleProviders = suiteModuleProviders;
        this.testMethodModuleProviders = testMethodModuleProviders;
        this.suiteLevelFulfillers = suiteLevelFulfillers;
        this.testMethodLevelFulfillers = testMethodLevelFulfillers;
        this.configuration = configuration;
    }

    @Override
    public void onStart(ITestContext context)
    {
        displayConfigurationToUser();

        Module suiteModule = combine(combine(getSuiteModules()), bind(suiteLevelFulfillers), bind(testMethodLevelFulfillers));
        GuiceTestContext initSuiteTestContext = new GuiceTestContext(suiteModule);
        TestContextStack<TestContext> suiteTextContextStack = new TestContextStack<>();
        suiteTextContextStack.push(initSuiteTestContext);

        try {
            Set<Requirement> allTestsRequirements = resolveAllTestsRequirements(context);
            doFulfillment(suiteTextContextStack, suiteLevelFulfillers, allTestsRequirements);
        }
        catch (RuntimeException e) {
            LOGGER.error("cannot initialize test suite", e);
            throw e;
        }

        setSuiteTestContextStack(suiteTextContextStack);
    }

    private void displayConfigurationToUser()
    {
        LOGGER.info("Configuration:");
        List<String> configurationKeys = Ordering.natural()
                .sortedCopy(configuration.listKeys());
        for (String key : configurationKeys) {
            LOGGER.info(String.format("%s -> %s", key, configuration.getString(key).orElse("<NOT SET>")));
        }
    }

    @Override
    public void onFinish(ITestContext context)
    {
        if (!suiteTestContextStack.isPresent()) {
            return;
        }

        TestStatus testStatus = context.getFailedTests().size() > 0 ? FAILURE : SUCCESS;
        doCleanup(suiteTestContextStack.get(), suiteLevelFulfillers, testStatus);
    }

    @Override
    public void onTestStart(ITestResult testResult)
    {
        setupLoggingMdcForTest(testResult);
        if (!suiteTestContextStack.isPresent()) {
            throw new SuiteInitializationException("test suite not initialized");
        }
        GuiceTestContext initTestContext = ((GuiceTestContext) suiteTestContextStack.get().peek()).createChildContext(emptyList(), getTestModules(testResult));
        TestContextStack<TestContext> testContextStack = new TestContextStack<>();
        testContextStack.push(initTestContext);

        try {
            Set<Requirement> testSpecificRequirements = getTestSpecificRequirements(testResult.getMethod());
            doFulfillment(testContextStack, testMethodLevelFulfillers, testSpecificRequirements);
        }
        catch (RuntimeException e) {
            LOGGER.debug("error within test initialization", e);
            throw e;
        }

        assertTestContextNotSet();
        pushAllTestContexts(testContextStack);
        TestContext topTestContext = testContextStack.peek();
        topTestContext.injectMembers(testResult.getInstance());

        runBeforeWithContextMethods(testResult, topTestContext);
    }

    @Override
    public void onTestSuccess(ITestResult result)
    {
        onTestFinished(result, SUCCESS);
    }

    @Override
    public void onTestFailure(ITestResult result)
    {
        LOGGER.debug("test failure", result.getThrowable());
        onTestFinished(result, FAILURE);
    }

    @Override
    public void onTestSkipped(ITestResult result)
    {
        onTestFinished(result, SUCCESS);
    }

    private void onTestFinished(ITestResult testResult, TestStatus testStatus)
    {
        if (!testContextIfSet().isPresent()) {
            return;
        }

        boolean runAfterSucceeded = false;
        try {
            runAfterWithContextMethods(testResult, testContext());
            runAfterSucceeded = true;
        }
        finally {
            TestContextStack<TestContext> testContextStack = popAllTestContexts();
            doCleanup(testContextStack, testMethodLevelFulfillers, runAfterSucceeded ? testStatus : FAILURE);
            cleanLoggingMdc();
        }
    }

    private void runBeforeWithContextMethods(ITestResult testResult, TestContext testContext)
    {
        try {
            invokeMethodsAnnotatedWith(BeforeTestWithContext.class, testResult, testContext);
        }
        catch (RuntimeException e) {
            TestContextStack<TestContext> testContextStack = popAllTestContexts();
            doCleanup(testContextStack, testMethodLevelFulfillers, FAILURE);
            throw e;
        }
    }

    private void runAfterWithContextMethods(ITestResult testResult, TestContext testContext)
    {
        invokeMethodsAnnotatedWith(AfterTestWithContext.class, testResult, testContext);
    }

    private void invokeMethodsAnnotatedWith(Class<? extends Annotation> annotationClass, ITestResult testCase, TestContext testContext)
    {
        for (Method declaredMethod : testCase.getTestClass().getRealClass().getDeclaredMethods()) {
            if (declaredMethod.getAnnotation(annotationClass) != null) {
                try {
                    declaredMethod.invoke(testCase.getInstance(), reflectionInjectorHelper.getMethodArguments(testContext, declaredMethod));
                }
                catch (IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeException("error invoking methods annotated with " + annotationClass.getName(), e);
                }
            }
        }
    }

    private void doFulfillment(TestContextStack<TestContext> testContextStack,
            List<Class<? extends RequirementFulfiller>> fulfillerClasses,
            Set<Requirement> requirements)
    {
        List<Class<? extends RequirementFulfiller>> successfulFulfillerClasses = newArrayList();

        try {
            for (Class<? extends RequirementFulfiller> fulfillerClass : fulfillerClasses) {
                LOGGER.debug("Fulfilling using {}", fulfillerClass);
                TestContext testContext = testContextStack.peek();
                runWithTestContext(testContext, () -> {
                    RequirementFulfiller fulfiller = testContext.getDependency(fulfillerClass);
                    TestContext testContextWithNewStates = testContext.createChildContext(fulfiller.fulfill(requirements));
                    successfulFulfillerClasses.add(fulfillerClass);
                    testContextStack.push(testContextWithNewStates);
                });
            }
        }
        catch (RuntimeException e) {
            LOGGER.debug("error during fulfillment", e);
            try {
                doCleanup(testContextStack, successfulFulfillerClasses, FAILURE);
            }
            catch (RuntimeException cleanupException) {
                e.addSuppressed(cleanupException);
            }
            throw e;
        }
    }

    private void doCleanup(TestContextStack<TestContext> testContextStack, List<Class<? extends RequirementFulfiller>> fulfillerClasses, TestStatus testStatus)
    {
        // one base test context plus one test context for each fulfiller
        checkState(testContextStack.size() == fulfillerClasses.size() + 1);

        for (Class<? extends RequirementFulfiller> fulfillerClass : reverse(fulfillerClasses)) {
            LOGGER.debug("Cleaning for fulfiller {}", fulfillerClass);
            TestContext testContext = testContextStack.pop();
            testContext.close();
            runWithTestContext(testContext, () -> testContextStack.peek().getDependency(fulfillerClass).cleanup(testStatus));
        }

        if (testContextStack.size() == 1) {
            // we are going to close last context, so we need to close TableManager's first
            testContextStack.peek().getOptionalDependency(TableManagerDispatcher.class)
                    .ifPresent(dispatcher -> dispatcher.getAllTableManagers().forEach(TableManager::close));
        }

        // remove close init test context too
        testContextStack.peek().close();
    }

    private List<Module> getSuiteModules()
    {
        return suiteModuleProviders
                .stream()
                .map(provider -> provider.getModule(configuration))
                .collect(toImmutableList());
    }

    private List<Module> getTestModules(ITestResult testResult)
    {
        return testMethodModuleProviders
                .stream()
                .map(provider -> provider.getModule(configuration, testResult))
                .collect(toImmutableList());
    }

    private <T> Module bind(List<Class<? extends T>> classes)
    {
        Function<Class<? extends T>, Module> bindToModule = clazz -> (Binder binder) -> binder.bind(clazz).in(Singleton.class);
        List<Module> modules = classes.stream()
                .map(bindToModule)
                .collect(toImmutableList());
        return combine(modules);
    }

    private Set<Requirement> resolveAllTestsRequirements(ITestContext context)
    {
        // we cannot assume that context contains RequirementsAwareTestNGMethod instances here
        // as interceptor is for some reason called after onStart() which uses this method.
        Set<Requirement> allTestsRequirements = Sets.newHashSet();
        for (ITestNGMethod iTestNGMethod : context.getAllTestMethods()) {
            Set<Set<Requirement>> requirementsSets = new TestSpecificRequirementsResolver(configuration).resolve(iTestNGMethod);
            for (Set<Requirement> requirementsSet : requirementsSets) {
                allTestsRequirements.addAll(requirementsSet);
            }
        }
        return allTestsRequirements;
    }

    private Set<Requirement> getTestSpecificRequirements(ITestNGMethod testMethod)
    {
        return ((RequirementsAwareTestNGMethod) testMethod).getRequirements();
    }

    private void setSuiteTestContextStack(TestContextStack<TestContext> suiteTestContextStack)
    {
        checkState(!this.suiteTestContextStack.isPresent(), "suite fulfillment result already set");
        this.suiteTestContextStack = Optional.of(suiteTestContextStack);
    }

    @Override
    public void onTestFailedButWithinSuccessPercentage(ITestResult result)
    {
    }

    private static class SuiteInitializationException
            extends RuntimeException
    {
        private static final AtomicLong instanceCount = new AtomicLong();

        SuiteInitializationException(String message)
        {
            super(
                    message,
                    null,
                    true,
                    // Suppress stacktrace for all but first 10 exceptions. It is not useful when printed for every test.
                    instanceCount.getAndIncrement() < 10);
        }
    }
}
