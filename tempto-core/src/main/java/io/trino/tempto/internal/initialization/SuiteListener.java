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

import io.trino.tempto.Requirement;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;
import org.slf4j.Logger;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.tempto.fulfillment.TestStatus.FAILURE;
import static io.trino.tempto.fulfillment.TestStatus.SUCCESS;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * JUnit Platform listener that drives the suite-level Tempto lifecycle. It resolves the union of all
 * test requirements from the {@link TestPlan}, performs suite-level fulfillment before any test runs
 * and tears it down once the whole plan completes.
 * <p>
 * Registered for every Launcher via {@code META-INF/services/org.junit.platform.launcher.TestExecutionListener}.
 * It is a no-op for plans that do not contain any Tempto tests, so it does not interfere with plain
 * JUnit tests sharing the same JVM.
 */
public class SuiteListener
        implements TestExecutionListener
{
    private static final Logger LOGGER = getLogger(SuiteListener.class);

    private final AtomicBoolean active = new AtomicBoolean(false);
    private volatile boolean anyFailure;

    @Override
    public void testPlanExecutionStarted(TestPlan testPlan)
    {
        if (!AllTestsRequirementsResolver.containsTemptoTests(testPlan)) {
            return;
        }
        active.set(true);
        anyFailure = false;
        try {
            TemptoRuntime runtime = TemptoRuntime.createRuntime();
            Set<Requirement> allRequirements = AllTestsRequirementsResolver.resolveAllRequirements(testPlan, runtime.getConfiguration());
            SuiteState.initialize(runtime, allRequirements);
        }
        catch (RuntimeException e) {
            // Leave the suite state unset so every test fails fast in TemptoTestExtension.beforeEach
            // with a SuiteInitializationException rather than running against a half-initialized suite.
            LOGGER.error("cannot initialize test suite", e);
        }
    }

    @Override
    public void executionFinished(TestIdentifier testIdentifier, TestExecutionResult testExecutionResult)
    {
        if (testExecutionResult.getStatus() == TestExecutionResult.Status.FAILED) {
            anyFailure = true;
        }
    }

    @Override
    public void testPlanExecutionFinished(TestPlan testPlan)
    {
        if (!active.get()) {
            return;
        }
        SuiteState.close(anyFailure ? FAILURE : SUCCESS);
    }
}
