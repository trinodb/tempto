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
import com.google.common.collect.ImmutableList;
import io.trino.tempto.internal.listeners.TestNameGroupNameMethodSelector;
import org.junit.platform.engine.discovery.ClassNameFilter;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.TagFilter;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.TestNG;
import org.testng.xml.XmlClass;
import org.testng.xml.XmlPackage;
import org.testng.xml.XmlSuite;
import org.testng.xml.XmlTest;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Lists.newArrayList;
import static io.trino.tempto.internal.configuration.TestConfigurationFactory.TEST_CONFIGURATION_URIS_KEY;
import static io.trino.tempto.internal.convention.ConventionTestsUtils.CONVENTION_TESTS_DIR_KEY;
import static io.trino.tempto.internal.convention.ConventionTestsUtils.CONVENTION_TESTS_RESULTS_DUMP_PATH_KEY;
import static io.trino.tempto.internal.listeners.TestNameGroupNameMethodSelector.TEST_GROUPS_TO_EXCLUDE_PROPERTY;
import static io.trino.tempto.internal.listeners.TestNameGroupNameMethodSelector.TEST_GROUPS_TO_RUN_PROPERTY;
import static io.trino.tempto.internal.listeners.TestNameGroupNameMethodSelector.TEST_NAMES_TO_EXCLUDE_PROPERTY;
import static io.trino.tempto.internal.listeners.TestNameGroupNameMethodSelector.TEST_NAMES_TO_RUN_PROPERTY;
import static java.util.Collections.singletonList;
import static org.junit.platform.launcher.TagFilter.includeTags;
import static org.testng.xml.XmlSuite.ParallelMode.getValidParallel;

public class TemptoRunner
{
    private static final Logger LOG = LoggerFactory.getLogger(TemptoRunner.class);
    private static final int METHOD_SELECTOR_PRIORITY = 20;
    private static final String METHOD_SELECTOR_CLASS_NAME = TestNameGroupNameMethodSelector.class.getName();
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
        if (!testWithJUnit() || !testWithTestNg()) {
            // tempto-runner is a CLI tool. It has to fail when there are test failures. That way CI step will be marked as failed.
            System.exit(1);
        }
    }

    private boolean testWithTestNg()
    {
        XmlSuite testSuite = getXmlSuite();
        testSuite.setThreadCount(options.getThreadCount());
        System.setProperty(CONVENTION_TESTS_DIR_KEY, options.getConventionTestsDirectory());
        TestNG testNG = new TestNG();
        testNG.setXmlSuites(singletonList(testSuite));
        options.getParallel().ifPresent(parallel -> testNG.setParallel(getValidParallel(parallel)));
        testNG.setOutputDirectory(options.getReportDir());
        setupTestsFiltering(testNG);
        options.getConventionResultsDumpPath()
                .ifPresent(path -> System.setProperty(CONVENTION_TESTS_RESULTS_DUMP_PATH_KEY, path));
        testNG.run();
        return !testNG.hasFailure();
    }

    private boolean testWithJUnit()
    {
        // TODO how set setOutputDirectory in Junit 5?
//        testNG.setOutputDirectory(options.getReportDir());
        LauncherDiscoveryRequestBuilder requestBuilder = LauncherDiscoveryRequestBuilder.request();
        options.getTestsPackage().stream()
                .map(x -> x.replace(".*", "")) // TODO remove. Temporary to comply with TestNG: package pattern
                .map(DiscoverySelectors::selectPackage).forEach(requestBuilder::selectors);
        options.getTests().stream().map(DiscoverySelectors::selectClass).forEach(requestBuilder::selectors);
        options.getExcludedTests().stream().map(ClassNameFilter::excludeClassNamePatterns).forEach(requestBuilder::filters);
        if (!options.getTestGroups().isEmpty()) {
            requestBuilder.filters(includeTags(ImmutableList.copyOf(options.getTestGroups())));
        }
        options.getExcludeGroups().stream().map(TagFilter::excludeTags).forEach(requestBuilder::filters);

        LauncherDiscoveryRequest request = requestBuilder
// TODO               ConventionBasedTestFactory is still testng
//                .selectors(selectClass("io.trino.tempto.internal.convention.ConventionBasedTestFactory"))
                .configurationParameter("junit.jupiter.execution.parallel.enabled", "true")
                .configurationParameter("junit.jupiter.execution.parallel.config.fixed.parallelism", Integer.toString(options.getThreadCount()))
                .build();

        // Configure the Launcher and listener
        SummaryGeneratingListener listener = new SummaryGeneratingListener();
        LauncherFactory.create().execute(request, listener);
        return listener.getSummary().getTestsFailedCount() == 0;
    }

    private void setupTestsConfiguration()
    {
        System.setProperty(TEST_CONFIGURATION_URIS_KEY, options.getConfigFiles());
    }

    private void setupTestsFiltering(TestNG testNG)
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
        testNG.addMethodSelector(METHOD_SELECTOR_CLASS_NAME, METHOD_SELECTOR_PRIORITY);
    }

    private XmlSuite getXmlSuite()
    {
        // we cannot use singletonLists here as testNG later
        // modifies lists stored in XmlSuite ... zonk
        XmlSuite testSuite = new XmlSuite();
        testSuite.setName("tempto-tests");
        testSuite.setFileName("tempto-tests");
        XmlTest test = new XmlTest(testSuite);
        test.setName("all");
        List<XmlPackage> testPackages = options.getTestsPackage().stream()
                .map(XmlPackage::new)
                .collect(toImmutableList());
        test.setPackages(testPackages);
        XmlClass conventionBasedTestsClass = new XmlClass("io.trino.tempto.internal.convention.ConventionBasedTestFactory");
        List<XmlClass> classes = newArrayList(conventionBasedTestsClass);
        test.setClasses(classes);
        test.setParallel(XmlSuite.ParallelMode.METHODS);
        return testSuite;
    }
}
