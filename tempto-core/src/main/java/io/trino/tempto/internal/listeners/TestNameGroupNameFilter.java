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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.platform.engine.FilterResult;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.support.descriptor.ClassSource;
import org.junit.platform.engine.support.descriptor.MethodSource;
import org.junit.platform.launcher.PostDiscoveryFilter;

import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.indexOf;
import static com.google.common.collect.Sets.intersection;
import static java.lang.System.getProperty;
import static java.util.Objects.requireNonNull;

/**
 * Selects tests on a per testName/testGroup basis. Groups map to JUnit tags; for convention
 * (dynamically generated) tests, which are invisible to discovery, the same matching is applied
 * in-process via {@link #matches(String, Set)}.
 * <p>
 * Governed by the following Java system properties:
 * <ul>
 * <li>{@value #TEST_GROUPS_TO_RUN_PROPERTY} - comma separated list of groups from which tests should be run
 * <li>{@value #TEST_GROUPS_TO_EXCLUDE_PROPERTY} - comma separated list of groups from which tests should be excluded
 * <li>{@value #TEST_NAMES_TO_RUN_PROPERTY} - comma separated list of test names to be run
 * <li>{@value #TEST_NAMES_TO_EXCLUDE_PROPERTY} - comma separated list of test names to be excluded
 * </ul>
 * <p>
 * For inclusion, test name matching is done by verifying if the value from the system property is a
 * substring of the actual test name. When excluding tests, an exact match is performed, i.e. the
 * fully qualified class name must be provided (and exact method name, when excluding individual methods).
 */
public class TestNameGroupNameFilter
        implements PostDiscoveryFilter
{
    public static final String TEST_NAMES_TO_RUN_PROPERTY = "io.trino.tempto.tests";
    public static final String TEST_NAMES_TO_EXCLUDE_PROPERTY = "io.trino.tempto.exclude_tests";
    public static final String TEST_GROUPS_TO_RUN_PROPERTY = "io.trino.tempto.groups";
    public static final String TEST_GROUPS_TO_EXCLUDE_PROPERTY = "io.trino.tempto.exclude_groups";

    private static final Splitter LIST_SYSTEM_PROPERTY_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private final Optional<Set<String>> testNamesToRun;
    private final Set<String> testNamesToExclude;
    private final Optional<Set<String>> testGroupsToRun;
    private final Set<String> testGroupsToExclude;

    public static TestNameGroupNameFilter fromSystemProperties()
    {
        return new TestNameGroupNameFilter(
                getOptionalSystemProperty(TEST_NAMES_TO_RUN_PROPERTY),
                getSetSystemProperty(TEST_NAMES_TO_EXCLUDE_PROPERTY),
                getOptionalSystemProperty(TEST_GROUPS_TO_RUN_PROPERTY),
                getSetSystemProperty(TEST_GROUPS_TO_EXCLUDE_PROPERTY));
    }

    public TestNameGroupNameFilter(
            Optional<Set<String>> testNamesToRun,
            Set<String> testNamesToExclude,
            Optional<Set<String>> testGroupsToRun,
            Set<String> testGroupsToExclude)
    {
        this.testNamesToRun = requireNonNull(testNamesToRun, "testNamesToRun is null");
        this.testNamesToExclude = ImmutableSet.copyOf(requireNonNull(testNamesToExclude, "testNamesToExclude is null"));
        this.testGroupsToRun = requireNonNull(testGroupsToRun, "testGroupsToRun is null");
        this.testGroupsToExclude = ImmutableSet.copyOf(requireNonNull(testGroupsToExclude, "testGroupsToExclude is null"));
    }

    @Override
    public FilterResult apply(TestDescriptor descriptor)
    {
        if (!descriptor.isTest()) {
            // Containers (classes, factories) are kept; their children are filtered individually.
            return FilterResult.included("container");
        }
        Optional<String> testName = testName(descriptor);
        if (testName.isEmpty()) {
            // No usable source (e.g. dynamic test placeholder) - leave it to in-process filtering.
            return FilterResult.included("no source");
        }
        Set<String> groups = descriptor.getTags().stream()
                .map(tag -> tag.getName())
                .collect(toImmutableSet());
        return matches(testName.get(), groups)
                ? FilterResult.included("matched tempto filter")
                : FilterResult.excluded("did not match tempto filter");
    }

    /**
     * Applies the same selection logic to an explicit test name and set of groups. Used for
     * convention tests, which are generated at execution time and are not visible to discovery.
     */
    public boolean matches(String testName, Set<String> testGroups)
    {
        return includeBasedOnTestName(testName) &&
                !excludeBasedOnName(testName) &&
                includeBasedOnGroups(testGroups) &&
                !excludeBasedOnGroups(testGroups);
    }

    private boolean includeBasedOnTestName(String testName)
    {
        return testNamesToRun.map(strings -> indexOf(strings, testName::contains) != -1).orElse(true);
    }

    private boolean excludeBasedOnName(String testName)
    {
        return testNamesToExclude.stream().anyMatch(exclusion -> matchesExclusion(testName, exclusion));
    }

    private boolean includeBasedOnGroups(Set<String> testGroups)
    {
        return testGroupsToRun.map(strings -> !intersection(testGroups, strings).isEmpty()).orElse(true);
    }

    private boolean excludeBasedOnGroups(Set<String> testGroups)
    {
        return !intersection(testGroups, testGroupsToExclude).isEmpty();
    }

    private static boolean matchesExclusion(String testName, String exclusion)
    {
        if (testName.equals(exclusion)) {
            return true;
        }
        return testName.startsWith(exclusion) && testName.lastIndexOf('.') == exclusion.length();
    }

    private static Optional<String> testName(TestDescriptor descriptor)
    {
        return descriptor.getSource().flatMap(source -> {
            if (source instanceof MethodSource methodSource) {
                return Optional.of(methodSource.getClassName() + "." + methodSource.getMethodName());
            }
            if (source instanceof ClassSource classSource) {
                return Optional.of(classSource.getClassName());
            }
            return Optional.empty();
        });
    }

    private static Set<String> getSetSystemProperty(String property)
    {
        return getOptionalSystemProperty(property).orElseGet(ImmutableSet::of);
    }

    private static Optional<Set<String>> getOptionalSystemProperty(String property)
    {
        return Optional.ofNullable(getProperty(property))
                .map(LIST_SYSTEM_PROPERTY_SPLITTER::split)
                .map(Sets::newHashSet);
    }
}
