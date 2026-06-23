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

import io.trino.tempto.internal.listeners.TestMetadata;
import io.trino.tempto.internal.listeners.TestMetadataReader;
import io.trino.tempto.internal.listeners.TestNameGroupNameMethodSelector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testng.IMethodSelectorContext;
import org.testng.ITestNGMethod;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestNameGroupNameMethodSelectorTest
{
    @ParameterizedTest
    @MethodSource("testSelectorMatchData")
    public void testSelectorMatch(
            String testName,
            List<String> testGroups,
            List<String> allowedTestNames,
            List<String> excludedTestNames,
            List<String> allowedTestGroups,
            List<String> excludedTestGroups,
            boolean expected)
    {
        TestMetadataReader metadataReader = mock(TestMetadataReader.class);
        when(metadataReader.readTestMetadata(any(ITestNGMethod.class)))
                .thenReturn(new TestMetadata(Set.copyOf(testGroups), testName, null));
        TestNameGroupNameMethodSelector testSelector = new TestNameGroupNameMethodSelector(
                asSetOptional(allowedTestNames),
                asSet(excludedTestNames),
                asSetOptional(allowedTestGroups),
                asSet(excludedTestGroups),
                metadataReader);

        assertThat(testSelector.includeMethod(mock(IMethodSelectorContext.class), mock(ITestNGMethod.class), true))
                .isEqualTo(expected);
    }

    @Test
    public void configurationMethodsAreAlwaysIncludedWithoutReadingMetadata()
    {
        // Configuration methods (e.g. @BeforeClass) are evaluated by the selector before their owning
        // ITestClass is attached, so ITestNGMethod.getTestClass() is null and reading their metadata throws
        // a NullPointerException. They must be included regardless of test name/group filters and without
        // touching the metadata reader. Otherwise @BeforeClass collection fails and the suite ends with
        // "No tests executed".
        TestMetadataReader metadataReader = mock(TestMetadataReader.class);
        when(metadataReader.readTestMetadata(any(ITestNGMethod.class)))
                .thenThrow(new NullPointerException("getTestClass() is null for configuration methods"));
        TestNameGroupNameMethodSelector testSelector = new TestNameGroupNameMethodSelector(
                asSetOptional(asList("someTestThatDoesNotMatch")),
                asSet(null),
                asSetOptional(asList("groupThatDoesNotMatch")),
                asSet(null),
                metadataReader);

        boolean isTestMethod = false;
        assertThat(testSelector.includeMethod(mock(IMethodSelectorContext.class), mock(ITestNGMethod.class), isTestMethod))
                .isTrue();
    }

    static Stream<Arguments> testSelectorMatchData()
    {
        return Stream.of(
                Arguments.of("abc", asList("g1", "g2"), null, null, null, null, true),
                Arguments.of("abc", asList("g1", "g2"), asList("abc"), null, null, null, true),
                Arguments.of("abc", asList("g1", "g2"), asList("xyz", "abc"), null, null, null, true),
                Arguments.of("abc", asList("g1", "g2"), null, null, asList("g1"), null, true),
                Arguments.of("abc", asList("g1", "g2"), null, null, asList("g1", "g3"), null, true),
                Arguments.of("abc", asList("g1", "g2"), asList("xyz", "abc"), null, asList("g1", "g3"), null, true),
                Arguments.of("p.q.r.abc", asList(), asList("abc"), null, null, null, true),
                Arguments.of("p.q.r.abc", asList(), asList("r.abc"), null, null, null, true),
                Arguments.of("p.q.r.abc", asList(), asList("p.q.r.abc"), null, null, null, true),
                Arguments.of("p.q.r.abc", asList(), asList("bc"), null, null, null, true),
                Arguments.of("p.q.r.abc", asList(), asList("xbc"), null, null, null, false),
                Arguments.of("abc", asList("g1", "g2"), asList("xyz", "abc"), null, asList("g1", "g3"), asList("g1"), false),
                Arguments.of("abc", asList("g1", "g2"), asList("xyz", "abc"), null, asList("g1", "g3"), asList("g2"), false),
                Arguments.of("abc", asList("g1", "g2"), asList("xyz", "abc"), asList("qwe"), asList("g1", "g3"), asList("g5"), true),
                Arguments.of("abc", asList("g1", "g2"), asList("xyz", "abc"), asList("ab"), asList("g1", "g3"), asList("g5"), true),
                Arguments.of("abc", asList("g1", "g2"), null, asList("abc"), asList("g1", "g3"), asList("g5"), false),
                Arguments.of("abc", asList("g1", "g2"), asList("xyz", "abc"), asList("abc"), asList("g1", "g3"), asList("g5"), false),
                Arguments.of("p.q.r.abc", asList("g1", "g2"), null, asList("p.q.r.xyz"), asList("g1", "g3"), asList("g5"), true),
                Arguments.of("p.q.r.abc", asList("g1", "g2"), null, asList("p.q.r.ab"), asList("g1", "g3"), asList("g5"), true),
                Arguments.of("p.q.r.abc", asList("g1", "g2"), null, asList("p.q.r.abc"), asList("g1", "g3"), asList("g5"), false),
                Arguments.of("p.q.r.abc", asList("g1", "g2"), null, asList("p.q.r"), asList("g1", "g3"), asList("g5"), false));
    }

    private static Optional<Set<String>> asSetOptional(List<String> strings)
    {
        if (strings == null) {
            return Optional.empty();
        }
        else {
            return Optional.of(Set.copyOf(strings));
        }
    }

    private static Set<String> asSet(List<String> strings)
    {
        if (strings == null) {
            return Set.of();
        }
        else {
            return Set.copyOf(strings);
        }
    }
}
