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

import com.google.common.base.Joiner;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;

/**
 * Logs test progress (start, outcome and a final summary) over a whole {@link TestPlan}.
 * Registered for every Launcher via
 * {@code META-INF/services/org.junit.platform.launcher.TestExecutionListener}.
 */
public class ProgressLoggingListener
        implements TestExecutionListener
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ProgressLoggingListener.class);

    private final AtomicInteger started = new AtomicInteger();
    private final AtomicInteger succeeded = new AtomicInteger();
    private final AtomicInteger skipped = new AtomicInteger();
    private final AtomicInteger failed = new AtomicInteger();
    private final ConcurrentHashMap<String, Long> testStartTimes = new ConcurrentHashMap<>();

    private volatile long startTime;
    private volatile long total;

    @Override
    public void testPlanExecutionStarted(TestPlan testPlan)
    {
        startTime = currentTimeMillis();
        total = testPlan.countTestIdentifiers(TestIdentifier::isTest);
        LOGGER.info("Starting tests");
    }

    @Override
    public void executionStarted(TestIdentifier testIdentifier)
    {
        if (!testIdentifier.isTest()) {
            return;
        }
        testStartTimes.put(testIdentifier.getUniqueId(), currentTimeMillis());
        int index = started.incrementAndGet();
        LOGGER.info("[{} of {}] {}", index, total, formatTestName(testIdentifier));
    }

    @Override
    public void executionSkipped(TestIdentifier testIdentifier, String reason)
    {
        if (!testIdentifier.isTest()) {
            return;
        }
        skipped.incrementAndGet();
        LOGGER.info("SKIPPED     /    {} ({})", formatTestName(testIdentifier), reason);
    }

    @Override
    public void executionFinished(TestIdentifier testIdentifier, TestExecutionResult testExecutionResult)
    {
        if (!testIdentifier.isTest()) {
            return;
        }
        long executionTime = elapsed(testIdentifier);
        switch (testExecutionResult.getStatus()) {
            case SUCCESSFUL -> {
                succeeded.incrementAndGet();
                LOGGER.info("SUCCESS     /    {} took {}", formatTestName(testIdentifier), formatDuration(executionTime));
            }
            case ABORTED -> {
                skipped.incrementAndGet();
                LOGGER.info("ABORTED     /    {} took {}", formatTestName(testIdentifier), formatDuration(executionTime));
            }
            case FAILED -> {
                failed.incrementAndGet();
                LOGGER.info("FAILURE     /    {} took {}", formatTestName(testIdentifier), formatDuration(executionTime));
                testExecutionResult.getThrowable().ifPresent(throwable -> LOGGER.error("Failure cause:", throwable));
            }
        }
    }

    @Override
    public void testPlanExecutionFinished(TestPlan testPlan)
    {
        if (started.get() + failed.get() + skipped.get() == 0) {
            return;
        }
        LOGGER.info("");
        LOGGER.info("Completed {} tests", started.get());
        LOGGER.info("{} SUCCEEDED      /      {} FAILED      /      {} SKIPPED", succeeded.get(), failed.get(), skipped.get());
        LOGGER.info("Tests execution took {}", formatDuration(currentTimeMillis() - startTime));
    }

    private long elapsed(TestIdentifier testIdentifier)
    {
        Long start = testStartTimes.remove(testIdentifier.getUniqueId());
        return start == null ? 0 : currentTimeMillis() - start;
    }

    private static String formatTestName(TestIdentifier testIdentifier)
    {
        String groups = Joiner.on(", ").join(testIdentifier.getTags().stream().map(tag -> tag.getName()).toList());
        return format("%s (Groups: %s)", testIdentifier.getDisplayName(), groups);
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
