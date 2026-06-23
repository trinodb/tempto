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

import io.trino.tempto.fulfillment.TestStatus;
import io.trino.tempto.initialization.TestMethodInfo;
import io.trino.tempto.internal.initialization.TemptoTestRunner.RunningTest;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;

import java.util.concurrent.atomic.AtomicLong;

import static io.trino.tempto.fulfillment.TestStatus.FAILURE;
import static io.trino.tempto.fulfillment.TestStatus.SUCCESS;

/**
 * Shared JUnit 5 lifecycle for Tempto tests: before each test it builds the per-test context and
 * fulfills requirements (via {@link TemptoTestRunner}); after each test it tears them down. The only
 * thing subclasses provide is how to describe the test ({@link #testMethodInfo}), which differs for
 * regular product tests (read from the {@link ExtensionContext}) and convention-based tests (read
 * from the generated {@link io.trino.tempto.internal.convention.ConventionBasedTest}).
 */
public abstract class AbstractTemptoTestExtension
        implements BeforeEachCallback, AfterEachCallback
{
    private static final Namespace NAMESPACE = Namespace.create(AbstractTemptoTestExtension.class);
    private static final String RUNNING_TEST_KEY = "runningTest";
    private static final String RUNNER_KEY = "runner";

    @Override
    public void beforeEach(ExtensionContext context)
    {
        SuiteState suiteState = SuiteState.instance()
                .orElseThrow(() -> new SuiteInitializationException("test suite not initialized"));

        TemptoTestRunner runner = new TemptoTestRunner(suiteState.getRuntime());
        RunningTest runningTest = runner.setUp(suiteState.getSuiteContext(), testMethodInfo(context));

        Store store = context.getStore(NAMESPACE);
        store.put(RUNNER_KEY, runner);
        store.put(RUNNING_TEST_KEY, runningTest);
    }

    @Override
    public void afterEach(ExtensionContext context)
    {
        Store store = context.getStore(NAMESPACE);
        RunningTest runningTest = store.get(RUNNING_TEST_KEY, RunningTest.class);
        TemptoTestRunner runner = store.get(RUNNER_KEY, TemptoTestRunner.class);
        if (runningTest == null || runner == null) {
            return;
        }
        TestStatus status = context.getExecutionException().isPresent() ? FAILURE : SUCCESS;
        runner.tearDown(runningTest, status);
    }

    protected abstract TestMethodInfo testMethodInfo(ExtensionContext context);

    public static final class SuiteInitializationException
            extends RuntimeException
    {
        private static final AtomicLong instanceCount = new AtomicLong();

        SuiteInitializationException(String message)
        {
            super(message,
                    null,
                    true,
                    // Suppress stacktrace for all but first 10 exceptions. It is not useful when printed for every test.
                    instanceCount.getAndIncrement() < 10);
        }
    }
}
