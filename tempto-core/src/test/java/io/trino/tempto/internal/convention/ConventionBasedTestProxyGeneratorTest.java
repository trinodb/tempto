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

package io.trino.tempto.internal.convention;

import io.trino.tempto.Requirement;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.internal.DummyTestRequirement;
import io.trino.tempto.internal.convention.AnnotatedFileParser.SectionParsingResult;
import io.trino.tempto.internal.convention.sql.SqlQueryConventionBasedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class ConventionBasedTestProxyGeneratorTest
{
    private final ConventionBasedTestProxyGenerator proxyGenerator = new ConventionBasedTestProxyGenerator("io.trino.tempto");

    @Test
    public void testGenerateProxy()
            throws Exception
    {
        Path testFile = file("convention/sample-test/query1.sql");
        SqlQueryDescriptor queryDescriptor = new SqlQueryDescriptor(section(testFile));
        SqlResultDescriptor resultDescriptor = new SqlResultDescriptor(section(testFile));
        Requirement requirement = mock(Requirement.class);
        ConventionBasedTest testInstance = new SqlQueryConventionBasedTest(
                Optional.empty(),
                Optional.empty(),
                testFile,
                "test.prefix",
                1,
                5,
                queryDescriptor,
                resultDescriptor,
                requirement);

        ConventionBasedTest proxiedTest = proxyGenerator.generateProxy(testInstance);
        Class<? extends ConventionBasedTest> proxiedClass = proxiedTest.getClass();
        Method testMethod = proxiedClass.getMethod("query1_1");
        org.testng.annotations.Test testAnnotation = testMethod.getAnnotation(org.testng.annotations.Test.class);

        assertThat(proxiedTest.getRequirements(null)).isSameAs(requirement);
        assertThat(testAnnotation).isNotNull();
        assertThat(testAnnotation.enabled()).isTrue();
        assertThat(testAnnotation.groups()).containsOnly("tpch", "quarantine");
    }

    private Path file(String path)
    {
        return Paths.get(getClass().getClassLoader().getResource(path).getPath());
    }

    private SectionParsingResult section(Path file)
    {
        return getOnlyElement(new AnnotatedFileParser().parseFile(file));
    }

    @ParameterizedTest
    @MethodSource("testClassAndMethodNameData")
    public void testClassNameAndMethodNamesProperlyGenerated(String testName, String expectedClassName, String expectedMethodName)
    {
        ConventionBasedTest test = DummyConventionBasedTest.emptyTest(testName);
        ConventionBasedTest proxy = proxyGenerator.generateProxy(test);
        List<String> proxyMethodNames = Arrays.stream(proxy.getClass().getMethods())
                .map(Method::getName)
                .toList();

        assertThat(proxy.getClass().getName()).isEqualTo(expectedClassName);
        assertThat(proxyMethodNames).contains(expectedMethodName);
    }

    static Stream<Arguments> testClassAndMethodNameData()
    {
        return Stream.of(
                Arguments.of("a", "io.trino.tempto", "a"),
                Arguments.of(".a", "io.trino.tempto", "a"),
                Arguments.of("a.b", "io.trino.tempto.a", "b"),
                Arguments.of(".a.b", "io.trino.tempto.a", "b"),
                Arguments.of("a.b.c", "io.trino.tempto.b", "c"),
                Arguments.of("a.b.c.d", "io.trino.tempto.c", "d"),
                Arguments.of("a.b.9c.1d", "io.trino.tempto._9c", "_1d"),
                Arguments.of("a.b.ala ma kota.a-kot-ma ale", "io.trino.tempto.ala_ma_kota", "a_kot_ma_ale"));
    }

    private static class DummyConventionBasedTest
            extends ConventionBasedTest
    {
        private final Requirement requirement;
        private final String testName;
        private final Set<String> testGroups;

        DummyConventionBasedTest(Requirement requirement, String testName, Set<String> testGroups)
        {
            this.requirement = requirement;
            this.testName = testName;
            this.testGroups = testGroups;
        }

        @Override
        public void test() {}

        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return requirement;
        }

        @Override
        public String getTestName()
        {
            return testName;
        }

        @Override
        public Set<String> getTestGroups()
        {
            return testGroups;
        }

        static DummyConventionBasedTest emptyTest(String testName)
        {
            return new DummyConventionBasedTest(new DummyTestRequirement(null), testName, emptySet());
        }
    }
}
