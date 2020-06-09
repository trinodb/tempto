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

package io.prestosql.tempto.internal.listeners;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.testng.IMethodSelector;
import org.testng.IMethodSelectorContext;
import org.testng.ITestNGMethod;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.Iterables.indexOf;
import static com.google.common.collect.Sets.intersection;
import static java.lang.System.getProperty;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;

/**
 * We want to be able to select test methods to be run on per testName/testGroup basis.
 * We cannot use standard testNG methods to select based on group/methodName for that though.
 * We generate some tests from factories and we want those generated tests to
 * belong to different test groups. But we cannot assign testNG groups to them as those can only be defined
 * in static way through annotations.
 * <p>
 * So we introduce our own mechanism of tests selection.
 * This is governed by following java system properties:
 * <ul>
 * <li>{@value #TEST_GROUPS_TO_RUN_PROPERTY} - should contain comma separated list of groups from which tests should be run
 * <li>{@value #TEST_GROUPS_TO_EXCLUDE_PROPERTY} - should contain comma separated list of groups from which tests should be excluded
 * <li>{@value #TEST_NAMES_TO_RUN_PROPERTY} - should contain comma separated list of test names to be run
 * </ul>
 * <p>
 * <p>
 * TestName matching is done by verifying if value from system property is suffix of actual test name in question
 */
public class TestNameGroupNameMethodSelector
        implements IMethodSelector
{
    public static final String TEST_NAMES_TO_RUN_PROPERTY = "io.prestosql.tempto.tests";
    public static final String TEST_GROUPS_TO_RUN_PROPERTY = "io.prestosql.tempto.groups";
    public static final String TEST_GROUPS_TO_EXCLUDE_PROPERTY = "io.prestosql.tempto.exclude_groups";

    private final Optional<Set<String>> testNamesToRun;
    private final Optional<Set<String>> testGroupsToRun;
    private final Set<String> testGroupsToExclude;

    private static final Splitter LIST_SYSTEM_PROPERTY_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private final TestMetadataReader testMetadataReader;

    public TestNameGroupNameMethodSelector()
    {
        this(getOptionalSystemProperty(TEST_NAMES_TO_RUN_PROPERTY),
                getOptionalSystemProperty(TEST_GROUPS_TO_RUN_PROPERTY),
                getSetSystemProperty(TEST_GROUPS_TO_EXCLUDE_PROPERTY),
                new TestMetadataReader());
    }

    public TestNameGroupNameMethodSelector(
            Optional<Set<String>> testNamesToRun,
            Optional<Set<String>> testGroupsToRun,
            Set<String> testGroupsToExclude,
            TestMetadataReader testMetadataReader)
    {
        this.testNamesToRun = requireNonNull(testNamesToRun, "testNamesToRun is null");
        this.testGroupsToRun = requireNonNull(testGroupsToRun, "testGroupsToRun is null");
        this.testGroupsToExclude = ImmutableSet.copyOf(requireNonNull(testGroupsToExclude, "testGroupsToExclude is null"));
        this.testMetadataReader = requireNonNull(testMetadataReader, "testMetadataReader is null");
    }

    @Override
    public boolean includeMethod(IMethodSelectorContext context, ITestNGMethod method, boolean isTestMethod)
    {
        TestMetadata testMetadata = testMetadataReader.readTestMetadata(method);
        return includeBasedOnTestName(testMetadata) &&
                includeBasedOnGroups(testMetadata) &&
                !excludeBasedOnGroups(testMetadata);
    }

    private boolean includeBasedOnTestName(TestMetadata testMetadata)
    {
        return testNamesToRun.map(strings -> indexOf(strings, testMetadata.testName::contains) != -1).orElse(true);
    }

    private boolean includeBasedOnGroups(TestMetadata testMetadata)
    {
        return testGroupsToRun.map(strings -> !intersection(testMetadata.testGroups, strings).isEmpty()).orElse(true);
    }

    private boolean excludeBasedOnGroups(TestMetadata testMetadata)
    {
        return !intersection(testMetadata.testGroups, testGroupsToExclude).isEmpty();
    }

    @Override
    public void setTestMethods(List<ITestNGMethod> testMethods)
    {
    }

    private static Set<String> getSetSystemProperty(String testNamesToRunProperty)
    {
        String property = getProperty(testNamesToRunProperty);
        return property == null ?
                ImmutableSet.of() :
                Sets.newHashSet(LIST_SYSTEM_PROPERTY_SPLITTER.split(property));
    }

    private static Optional<Set<String>> getOptionalSystemProperty(String testNamesToRunProperty)
    {
        String property = getProperty(testNamesToRunProperty);
        return ofNullable(property)
                .map(LIST_SYSTEM_PROPERTY_SPLITTER::split)
                .map(Sets::newHashSet);
    }
}
