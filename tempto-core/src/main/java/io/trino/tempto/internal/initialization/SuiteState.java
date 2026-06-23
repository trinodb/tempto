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

import com.google.common.collect.Ordering;
import com.google.inject.Module;
import io.trino.tempto.Requirement;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.context.TestContext;
import io.trino.tempto.fulfillment.TestStatus;
import io.trino.tempto.internal.context.GuiceTestContext;
import org.slf4j.Logger;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.inject.util.Modules.combine;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Holds the suite-level {@link TestContext} (and its fulfilled state) that is shared by all tests in
 * the suite. Initialized once before any test runs and torn down after the whole suite finishes
 * (see {@link SuiteListener}). This is the JUnit equivalent of the suite lifecycle that used to live
 * in TestNG's {@code ITestListener.onStart}/{@code onFinish}.
 */
public final class SuiteState
{
    private static final Logger LOGGER = getLogger(SuiteState.class);

    private static volatile SuiteState instance;

    private final TemptoRuntime runtime;
    private final Deque<TestContext> suiteContextStack;

    private SuiteState(TemptoRuntime runtime, Deque<TestContext> suiteContextStack)
    {
        this.runtime = runtime;
        this.suiteContextStack = suiteContextStack;
    }

    public static synchronized void initialize(TemptoRuntime runtime, Set<Requirement> allTestsRequirements)
    {
        checkState(instance == null, "suite state already initialized");
        displayConfigurationToUser(runtime.getConfiguration());

        Module suiteModule = combine(
                combine(getSuiteModules(runtime)),
                Fulfillment.bindFulfillers(runtime.getSuiteLevelFulfillers()),
                Fulfillment.bindFulfillers(runtime.getTestMethodLevelFulfillers()));
        GuiceTestContext initSuiteTestContext = new GuiceTestContext(suiteModule);
        Deque<TestContext> suiteContextStack = new ArrayDeque<>();
        suiteContextStack.addLast(initSuiteTestContext);

        Fulfillment.fulfill(suiteContextStack, runtime.getSuiteLevelFulfillers(), allTestsRequirements);

        instance = new SuiteState(runtime, suiteContextStack);
    }

    public static Optional<SuiteState> instance()
    {
        return Optional.ofNullable(instance);
    }

    public static synchronized void close(TestStatus testStatus)
    {
        if (instance != null) {
            try {
                Fulfillment.cleanup(instance.suiteContextStack, instance.runtime.getSuiteLevelFulfillers(), testStatus);
            }
            finally {
                instance = null;
            }
        }
    }

    public TemptoRuntime getRuntime()
    {
        return runtime;
    }

    public TestContext getSuiteContext()
    {
        return suiteContextStack.getLast();
    }

    private static List<Module> getSuiteModules(TemptoRuntime runtime)
    {
        return runtime.getSuiteModuleProviders()
                .stream()
                .map(provider -> provider.getModule(runtime.getConfiguration()))
                .collect(toImmutableList());
    }

    private static void displayConfigurationToUser(Configuration configuration)
    {
        LOGGER.info("Configuration:");
        List<String> configurationKeys = Ordering.natural()
                .sortedCopy(configuration.listKeys());
        for (String key : configurationKeys) {
            LOGGER.info(String.format("%s -> %s", key, configuration.getString(key).orElse("<NOT SET>")));
        }
    }
}
