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

package io.trino.tempto.internal.listeners;

import io.airlift.log.Logging;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.System.currentTimeMillis;

public class ProgressLoggingListenerJUnit
        implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback, AfterAllCallback
{
    private final static Logger LOGGER = LoggerFactory.getLogger(ProgressLoggingListenerJUnit.class);

    static {
        Logging.initialize();
    }

    private int started;
    private int succeeded;
    private int failed;
    private long startTime;
    private long testStartTime;

    @Override
    public void beforeAll(ExtensionContext context)
    {
        startTime = currentTimeMillis();
        LOGGER.info("Starting tests");
    }

    @Override
    public void beforeEach(ExtensionContext context)
    {
        testStartTime = currentTimeMillis();
        started++;
        LOGGER.info("[{}] Starting test: {}", started, formatTestName(context));
    }

    @Override
    public void afterEach(ExtensionContext context)
    {
        long executionTime = currentTimeMillis() - testStartTime;
        if (context.getExecutionException().isPresent()) {
            failed++;
            LOGGER.info("FAILURE: {} took {}", formatTestName(context), formatDuration(executionTime));
            LOGGER.error("Failure cause:", context.getExecutionException().get());
        }
        else {
            succeeded++;
            LOGGER.info("SUCCESS: {} took {}", formatTestName(context), formatDuration(executionTime));
        }
    }

    @Override
    public void afterAll(ExtensionContext context)
    {
        checkState(succeeded + failed > 0, "No tests executed");
        LOGGER.info("");
        LOGGER.info("Completed {} tests", started);
        LOGGER.info("{} SUCCEEDED      /      {} FAILED", succeeded, failed);
        LOGGER.info("Tests execution took {}", formatDuration(currentTimeMillis() - startTime));
    }

    private String formatTestName(ExtensionContext context)
    {
//        TestMetadata testMetadata = testMetadataReader.readTestMetadata(testCase);
//        String testGroups = Joiner.on(", ").join(testMetadata.testGroups);
//        String testParameters = formatTestParameters(testMetadata.testParameters);
//
//        return format("%s%s (Groups: %s)", testMetadata.testName, testParameters, testGroups);
        return "%s#%s".formatted(context.getRequiredTestClass().getName(), context.getDisplayName());
    }

    private static String formatDuration(long durationInMillis)
    {
        BigDecimal durationSeconds = durationInSeconds(durationInMillis);
        if (durationSeconds.longValue() > 60) {
            long minutes = durationSeconds.longValue() / 60;
            long restSeconds = durationSeconds.longValue() % 60;
            return String.format("%d minutes and %d seconds", minutes, restSeconds);
        }
        else {
            return String.format("%s seconds", durationSeconds);
        }
    }

    private static BigDecimal durationInSeconds(long millis)
    {
        return new BigDecimal(millis).divide(new BigDecimal(1000), 1, RoundingMode.HALF_UP);
    }
}
