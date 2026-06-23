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

import com.google.inject.Inject;
import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.Requirement;
import io.trino.tempto.RequirementsProvider;
import io.trino.tempto.Requires;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.context.State;
import io.trino.tempto.context.TestContext;
import io.trino.tempto.context.TestContextCloseCallback;
import io.trino.tempto.fulfillment.RequirementFulfiller;
import io.trino.tempto.fulfillment.RequirementFulfiller.TestLevelFulfiller;
import io.trino.tempto.fulfillment.TestStatus;
import io.trino.tempto.initialization.TestMethodInfo;
import io.trino.tempto.internal.initialization.TemptoTestRunner.RunningTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.tempto.context.ThreadLocalTestContextHolder.assertTestContextNotSet;
import static io.trino.tempto.context.ThreadLocalTestContextHolder.assertTestContextSet;
import static io.trino.tempto.context.ThreadLocalTestContextHolder.popAllTestContexts;
import static io.trino.tempto.context.ThreadLocalTestContextHolder.testContextIfSet;
import static io.trino.tempto.fulfillment.TestStatus.FAILURE;
import static io.trino.tempto.fulfillment.TestStatus.SUCCESS;
import static io.trino.tempto.internal.configuration.EmptyConfiguration.emptyConfiguration;
import static org.assertj.core.api.Assertions.assertThat;

public class TemptoTestRunnerTest
{
    static final String A = "A";
    static final String B = "B";
    static final String C = "C";

    static final String SUITE_A_FULFILL = "AFULFILL";
    static final String TEST_B_FULFILL = "BFULFILL";
    static final String THROWING_TEST_C_FULFILL = "CFULFILL";

    static final String SUITE_A_CLEANUP = "ACLEANUP";
    static final String TEST_B_CLEANUP = "BCLEANUP";
    static final String THROWING_TEST_C_CLEANUP = "CCLEANUP";

    static final String SUITE_A_CALLBACK = "ACALLBACK";
    static final String TEST_B_CALLBACK = "BCALLBACK";
    static final String THROWING_TEST_C_CALLBACK = "CCALLBACK";

    static final String BEFORE_METHOD = "beforeMethod";
    static final String AFTER_METHOD = "afterMethod";
    public static final String BEFORE_METHOD_OVERRIDE = "beforeMethodOverride";
    public static final String BEFORE_METHOD_ADDITIONAL = "beforeMethodAdditional";
    public static final String AFTER_METHOD_OVERRIDE = "afterMethodOverride";
    public static final String AFTER_METHOD_ADDITIONAL = "afterMethodAdditional";

    static final DummyRequirement A_REQUIREMENT = new DummyRequirement(A);
    static final DummyRequirement B_REQUIREMENT = new DummyRequirement(B);
    static final DummyRequirement C_REQUIREMENT = new DummyRequirement(C);
    static List<Event> events;

    @BeforeEach
    public void setup()
    {
        events = new ArrayList<>();
    }

    @AfterEach
    public void cleanup()
    {
        // Defensive cleanup so a failing test does not leak suite/thread-local state into the next one.
        if (testContextIfSet().isPresent()) {
            popAllTestContexts();
        }
        SuiteState.close(FAILURE);
    }

    @ParameterizedTest
    @MethodSource("positiveFlowsData")
    public void positiveFlows(TestClass testClass, List<String> eventsList)
            throws Exception
    {
        TemptoRuntime runtime = runtime(List.of(AFulfiller.class), List.of(BFulfiller.class));
        SuiteState.initialize(runtime, Set.of(A_REQUIREMENT, B_REQUIREMENT));
        TemptoTestRunner runner = new TemptoTestRunner(runtime);

        assertTestContextNotSet();
        RunningTest runningTest = runner.setUp(SuiteState.instance().orElseThrow().getSuiteContext(), testMethodInfo(testClass, "testMethodSuccess"));
        assertTestContextSet();
        assertThat(testClass.testContext).isNotNull();
        runner.tearDown(runningTest, SUCCESS);
        assertTestContextNotSet();
        SuiteState.close(SUCCESS);

        assertThat(events.stream()
                .map(s -> s.name)
                .collect(Collectors.toList()))
                .isEqualTo(eventsList);
        assertThat(events.get(1).object).isEqualTo(events.get(events.size() - 4).object);
        assertThat(events.get(0).object).isEqualTo(events.get(events.size() - 2).object);
    }

    static Stream<Arguments> positiveFlowsData()
    {
        return Stream.of(
                Arguments.of(new TestClass(), List.of(
                        SUITE_A_FULFILL,
                        TEST_B_FULFILL,
                        BEFORE_METHOD,
                        AFTER_METHOD,
                        TEST_B_CLEANUP,
                        TEST_B_CALLBACK,
                        SUITE_A_CLEANUP,
                        SUITE_A_CALLBACK)),
                Arguments.of(new TestClassNoOverrideAnnotatedMethods(), List.of(
                        SUITE_A_FULFILL,
                        TEST_B_FULFILL,
                        BEFORE_METHOD,
                        AFTER_METHOD,
                        TEST_B_CLEANUP,
                        TEST_B_CALLBACK,
                        SUITE_A_CLEANUP,
                        SUITE_A_CALLBACK)),
                Arguments.of(new TestClassOverrideAnnotatedMethods(), List.of(
                        SUITE_A_FULFILL,
                        TEST_B_FULFILL,
                        BEFORE_METHOD_OVERRIDE,
                        AFTER_METHOD_OVERRIDE,
                        TEST_B_CLEANUP,
                        TEST_B_CALLBACK,
                        SUITE_A_CLEANUP,
                        SUITE_A_CALLBACK)),
                Arguments.of(new TestClassAdditionalAnnotatedMethod(), List.of(
                        SUITE_A_FULFILL,
                        TEST_B_FULFILL,
                        BEFORE_METHOD,
                        BEFORE_METHOD_ADDITIONAL,
                        AFTER_METHOD_ADDITIONAL,
                        AFTER_METHOD,
                        TEST_B_CLEANUP,
                        TEST_B_CALLBACK,
                        SUITE_A_CLEANUP,
                        SUITE_A_CALLBACK)));
    }

    @Test
    public void failureDuringFulfillment()
            throws Exception
    {
        TestClass testClass = new TestClass();
        TemptoRuntime runtime = runtime(List.of(AFulfiller.class), List.of(BFulfiller.class, CFulfiller.class));
        SuiteState.initialize(runtime, Set.of(A_REQUIREMENT, B_REQUIREMENT, C_REQUIREMENT));
        TemptoTestRunner runner = new TemptoTestRunner(runtime);

        try {
            runner.setUp(SuiteState.instance().orElseThrow().getSuiteContext(), testMethodInfo(testClass, "testMethodFailed"));
            assertThat(false).isTrue();
        }
        catch (RuntimeException ignored) {
        }
        assertTestContextNotSet();
        SuiteState.close(FAILURE);

        assertThat(events.get(0).name).isEqualTo(SUITE_A_FULFILL);
        assertThat(events.get(1).name).isEqualTo(TEST_B_FULFILL);
        assertThat(events.get(2).name).isEqualTo(THROWING_TEST_C_FULFILL);
        assertThat(events.get(3).name).isEqualTo(THROWING_TEST_C_CALLBACK);
        assertThat(events.get(4).name).isEqualTo(TEST_B_CLEANUP);
        assertThat(events.get(5).name).isEqualTo(TEST_B_CALLBACK);
        assertThat(events.get(6).name).isEqualTo(SUITE_A_CLEANUP);
        assertThat(events.get(7).name).isEqualTo(SUITE_A_CALLBACK);

        assertThat(events.get(1).object).isEqualTo(events.get(4).object);
        assertThat(events.get(0).object).isEqualTo(events.get(6).object);
    }

    private static TemptoRuntime runtime(
            List<Class<? extends RequirementFulfiller>> suiteLevelFulfillers,
            List<Class<? extends RequirementFulfiller>> testLevelFulfillers)
    {
        return new TemptoRuntime(List.of(), List.of(), suiteLevelFulfillers, testLevelFulfillers, emptyConfiguration());
    }

    private static TestMethodInfo testMethodInfo(TestClass testClass, String methodName)
            throws NoSuchMethodException
    {
        Method method = testClass.getClass().getMethod(methodName);
        return new TestMethodInfo(
                testClass.getClass().getName() + "." + methodName,
                Set.of(),
                testClass.getClass(),
                Optional.of(method),
                testClass);
    }

    static class TestClassNoOverrideAnnotatedMethods
            extends TestClass {}

    static class TestClassAdditionalAnnotatedMethod
            extends TestClass
    {
        @BeforeMethodWithContext
        public void beforeMethodAdditional()
        {
            events.add(new Event(BEFORE_METHOD_ADDITIONAL, this));
        }

        @AfterMethodWithContext
        public void afterMethodAdditional()
        {
            events.add(new Event(AFTER_METHOD_ADDITIONAL, this));
        }
    }

    static class TestClassOverrideAnnotatedMethods
            extends TestClass
    {
        @Override
        @BeforeMethodWithContext
        public void beforeMethod()
        {
            events.add(new Event(BEFORE_METHOD_OVERRIDE, this));
        }

        @Override
        @AfterMethodWithContext
        public void afterMethod()
        {
            events.add(new Event(AFTER_METHOD_OVERRIDE, this));
        }
    }

    @Requires(ARequirement.class)
    static class TestClass
            implements RequirementsProvider
    {
        @Inject
        TestContext testContext;

        @BeforeMethodWithContext
        public void beforeMethod()
        {
            events.add(new Event(BEFORE_METHOD, this));
        }

        @AfterMethodWithContext
        public void afterMethod()
        {
            events.add(new Event(AFTER_METHOD, this));
        }

        public void testMethodSuccess() {}

        @Requires(CRequirement.class)
        public void testMethodFailed() {}

        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return B_REQUIREMENT;
        }
    }

    static class ARequirement
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return A_REQUIREMENT;
        }
    }

    @TestLevelFulfiller
    static class AFulfiller
            extends DummyFulfiller
    {
        @Inject
        AFulfiller(TestContext testContext)
        {
            super(A, SUITE_A_FULFILL, SUITE_A_CLEANUP, SUITE_A_CALLBACK, testContext);
        }
    }

    @TestLevelFulfiller
    static class BFulfiller
            extends DummyFulfiller
    {
        @Inject
        BFulfiller(TestContext testContext)
        {
            super(B, TEST_B_FULFILL, TEST_B_CLEANUP, TEST_B_CALLBACK, testContext);
        }
    }

    static class CRequirement
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return C_REQUIREMENT;
        }
    }

    @TestLevelFulfiller
    static class CFulfiller
            extends DummyFulfiller
    {
        @Inject
        CFulfiller(TestContext testContext)
        {
            super(C, THROWING_TEST_C_FULFILL, THROWING_TEST_C_CLEANUP, THROWING_TEST_C_CALLBACK, testContext);
        }

        @Override
        public Set<State> fulfill(Set<Requirement> requirements)
        {
            super.fulfill(requirements);
            throw new RuntimeException();
        }
    }

    static class DummyFulfiller
            implements RequirementFulfiller
    {
        final String requirementName;
        final String fulfillEventName;
        final String cleanupEventName;

        DummyFulfiller(
                String requirementName,
                String fulfillEventName,
                String cleanupEventName,
                String callbackEventName,
                TestContext testContext)
        {
            this.requirementName = requirementName;
            this.fulfillEventName = fulfillEventName;
            this.cleanupEventName = cleanupEventName;

            registerCallback(testContext, callbackEventName);
        }

        void registerCallback(TestContext testContext, String callbackEventName)
        {
            testContext.registerCloseCallback(new TestContextCloseCallback()
            {
                @Override
                public void testContextClosed(TestContext testContext)
                {
                    events.add(new Event(callbackEventName, this));
                }
            });
        }

        @Override
        public Set<State> fulfill(Set<Requirement> requirements)
        {
            if (!requirements.contains(new DummyRequirement(requirementName))) {
                return Set.of();
            }

            events.add(new Event(fulfillEventName, this));
            return Set.of();
        }

        @Override
        public void cleanup(TestStatus testStatus)
        {
            events.add(new Event(cleanupEventName, this));
        }
    }

    static class DummyRequirement
            implements Requirement
    {
        final String name;

        DummyRequirement(String name)
        {
            this.name = name;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            DummyRequirement that = (DummyRequirement) o;

            return name == null ? that.name == null : name.equals(that.name);
        }

        @Override
        public int hashCode()
        {
            return name.hashCode();
        }
    }

    static class Event
    {
        final String name;
        final Object object;

        Event(String name, Object object)
        {
            this.name = name;
            this.object = object;
        }
    }
}
