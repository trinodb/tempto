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

package io.trino.tempto.runner;

import com.google.common.base.Joiner;
import io.trino.tempto.internal.convention.ConventionBasedTests;
import io.trino.tempto.internal.listeners.TestNameGroupNameFilter;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;
import org.junit.platform.reporting.legacy.xml.LegacyXmlReportGeneratingListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.nio.file.Paths;

import static io.trino.tempto.internal.configuration.TestConfigurationFactory.TEST_CONFIGURATION_URIS_KEY;
import static io.trino.tempto.internal.convention.ConventionTestsUtils.CONVENTION_TESTS_DIR_KEY;
import static io.trino.tempto.internal.convention.ConventionTestsUtils.CONVENTION_TESTS_RESULTS_DUMP_PATH_KEY;
import static io.trino.tempto.internal.listeners.TestNameGroupNameFilter.TEST_GROUPS_TO_EXCLUDE_PROPERTY;
import static io.trino.tempto.internal.listeners.TestNameGroupNameFilter.TEST_GROUPS_TO_RUN_PROPERTY;
import static io.trino.tempto.internal.listeners.TestNameGroupNameFilter.TEST_NAMES_TO_EXCLUDE_PROPERTY;
import static io.trino.tempto.internal.listeners.TestNameGroupNameFilter.TEST_NAMES_TO_RUN_PROPERTY;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectPackage;
import static org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder.request;

public class TemptoRunner
{
    private static final Logger LOG = LoggerFactory.getLogger(TemptoRunner.class);

    private final TemptoRunnerCommandLineParser parser;
    private final TemptoRunnerOptions options;

    public static void runTempto(TemptoRunnerCommandLineParser parser, String[] args)
    {
        TemptoRunnerOptions options = parser.parseCommandLine(args);
        try {
            TemptoRunner.runTempto(parser, options);
        }
        catch (TemptoRunnerCommandLineParser.ParsingException e) {
            System.err.println("Could not parse command line. " + e.getMessage());
            System.err.println();
            parser.printHelpMessage();
            System.exit(1);
        }
    }

    public static void runTempto(TemptoRunnerCommandLineParser parser, TemptoRunnerOptions options)
    {
        new TemptoRunner(parser, options).run();
    }

    private TemptoRunner(TemptoRunnerCommandLineParser parser, TemptoRunnerOptions options)
    {
        this.parser = parser;
        this.options = options;
    }

    private void run()
    {
        LOG.debug("running tempto with options: {}", options);
        if (options.isHelpRequested()) {
            parser.printHelpMessage();
            return;
        }

        setupTestsConfiguration();
        setupTestsFiltering();
        System.setProperty(CONVENTION_TESTS_DIR_KEY, options.getConventionTestsDirectory());
        options.getConventionResultsDumpPath()
                .ifPresent(path -> System.setProperty(CONVENTION_TESTS_RESULTS_DUMP_PATH_KEY, path));

        LauncherDiscoveryRequest request = buildDiscoveryRequest();
        Launcher launcher = LauncherFactory.create();

        SummaryGeneratingListener summaryListener = new SummaryGeneratingListener();
        LegacyXmlReportGeneratingListener reportListener = new LegacyXmlReportGeneratingListener(
                Paths.get(options.getReportDir()),
                new PrintWriter(System.out, true));
        launcher.registerTestExecutionListeners(summaryListener, reportListener);

        launcher.execute(request);

        TestExecutionSummary summary = summaryListener.getSummary();
        summary.printTo(new PrintWriter(System.out, true));
        if (summary.getTotalFailureCount() > 0) {
            summary.printFailuresTo(new PrintWriter(System.out, true));
            System.exit(1);
        }
    }

    private LauncherDiscoveryRequest buildDiscoveryRequest()
    {
        var builder = request();
        options.getTestsPackage().forEach(pkg -> builder.selectors(selectPackage(pkg)));
        // Convention-based (file driven) tests are exposed as JUnit dynamic tests by this class.
        builder.selectors(selectClass(ConventionBasedTests.class));
        builder.filters(TestNameGroupNameFilter.fromSystemProperties());

        boolean parallel = !options.getParallel().map("none"::equalsIgnoreCase).orElse(false);
        builder.configurationParameter("junit.jupiter.execution.parallel.enabled", Boolean.toString(parallel));
        if (parallel) {
            builder.configurationParameter("junit.jupiter.execution.parallel.mode.default", "concurrent");
            builder.configurationParameter("junit.jupiter.execution.parallel.mode.classes.default", "concurrent");
            builder.configurationParameter("junit.jupiter.execution.parallel.config.strategy", "fixed");
            builder.configurationParameter("junit.jupiter.execution.parallel.config.fixed.parallelism", Integer.toString(options.getThreadCount()));
        }
        return builder.build();
    }

    private void setupTestsConfiguration()
    {
        System.setProperty(TEST_CONFIGURATION_URIS_KEY, options.getConfigFiles());
    }

    private void setupTestsFiltering()
    {
        if (!options.getTestGroups().isEmpty()) {
            System.setProperty(TEST_GROUPS_TO_RUN_PROPERTY, Joiner.on(',').join(options.getTestGroups()));
        }
        if (!options.getExcludeGroups().isEmpty()) {
            System.setProperty(TEST_GROUPS_TO_EXCLUDE_PROPERTY, Joiner.on(',').join(options.getExcludeGroups()));
        }
        if (!options.getTests().isEmpty()) {
            System.setProperty(TEST_NAMES_TO_RUN_PROPERTY, Joiner.on(',').join(options.getTests()));
        }
        if (!options.getExcludedTests().isEmpty()) {
            System.setProperty(TEST_NAMES_TO_EXCLUDE_PROPERTY, Joiner.on(',').join(options.getExcludedTests()));
        }
    }
}
