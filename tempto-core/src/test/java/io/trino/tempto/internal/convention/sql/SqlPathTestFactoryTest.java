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

package io.trino.tempto.internal.convention.sql;

import io.trino.tempto.CompositeRequirement;
import io.trino.tempto.Requirement;
import io.trino.tempto.RequirementsProvider;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.fulfillment.table.TableDefinitionsRepository;
import io.trino.tempto.internal.convention.ConventionBasedTest;
import io.trino.tempto.internal.convention.ConventionBasedTestProxyGenerator;
import org.apache.commons.io.FilenameUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static io.trino.tempto.internal.configuration.EmptyConfiguration.emptyConfiguration;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

public class SqlPathTestFactoryTest
{
    @TempDir
    Path temporaryFolder;

    private SqlPathTestFactory sqlPathTestFactory;

    @BeforeEach
    public void setup()
    {
        TableDefinitionsRepository tableDefinitionsRepositoryMock = mock(TableDefinitionsRepository.class);
        ConventionBasedTestProxyGenerator conventionBasedTestProxyGeneratorMock = new ConventionBasedTestProxyGenerator("test");
        sqlPathTestFactory = new SqlPathTestFactory(tableDefinitionsRepositoryMock, conventionBasedTestProxyGeneratorMock, emptyConfiguration());
    }

    @Test
    public void shouldCreateConventionTestWithRequires()
            throws IOException
    {
        Path testPath = getPathForConventionTest("-- requires: " + DummyRequirementsProvider1.class.getName() + "; groups:foo");

        List<ConventionBasedTest> conventionBasedTests = sqlPathTestFactory.createTestsForPath(testPath, "tests.prefix", null);
        String baseTestFileName = FilenameUtils.getBaseName(testPath.getFileName().toString());

        assertThat(conventionBasedTests).hasSize(1);
        assertThat(containsRequirement(conventionBasedTests.get(0).getRequirements(emptyConfiguration()), DummyRequirement1.class)).isTrue();
        assertThat(conventionBasedTests.get(0).getTestName()).isEqualTo("tests.prefix." + baseTestFileName);
        assertThat(conventionBasedTests.get(0).getTestGroups()).isEqualTo(Set.of("foo"));
    }

    @Test
    public void shouldUseSectionNameAsTestName()
            throws IOException
    {
        Path testPath = getPathForConventionTest("-- name:foo_boo");

        List<ConventionBasedTest> conventionBasedTests = sqlPathTestFactory.createTestsForPath(testPath, "tests.prefix", null);
        String baseTestFileName = FilenameUtils.getBaseName(testPath.getFileName().toString());

        assertThat(conventionBasedTests).hasSize(1);
        assertThat(conventionBasedTests.get(0).getTestName()).isEqualTo("tests.prefix." + baseTestFileName + ".foo_boo");
    }

    @Test
    public void shouldCreateTestsWithMultipleSections()
            throws IOException
    {
        Path testPath = getPathForConventionTest(
                "\n"
                        + "-- requires: " + DummyRequirementsProvider1.class.getName() + "\n"
                        + "--! name: query_1; requires: " + DummyRequirementsProvider2.class.getName() + "\n"
                        + "query 1 sql\n"
                        + "--!\n"
                        + "query 1 result\n"
                        + "--! name: query_2\n"
                        + "query 2 sql\n"
                        + "--!\n"
                        + "query 2 result\n",
                Optional.empty());

        List<ConventionBasedTest> conventionBasedTests = sqlPathTestFactory.createTestsForPath(testPath, "tests.prefix", null);
        String testFileBaseName = FilenameUtils.getBaseName(testPath.getFileName().toString());

        assertThat(conventionBasedTests).hasSize(2);

        assertThat(conventionBasedTests.get(0).getTestName()).isEqualTo("tests.prefix." + testFileBaseName + ".query_1");
        assertThat(containsRequirement(conventionBasedTests.get(0).getRequirements(emptyConfiguration()), DummyRequirement1.class)).isTrue();
        assertThat(containsRequirement(conventionBasedTests.get(0).getRequirements(emptyConfiguration()), DummyRequirement2.class)).isTrue();

        assertThat(conventionBasedTests.get(1).getTestName()).isEqualTo("tests.prefix." + testFileBaseName + ".query_2");
        assertThat(containsRequirement(conventionBasedTests.get(1).getRequirements(emptyConfiguration()), DummyRequirement1.class)).isTrue();
        assertThat(containsRequirement(conventionBasedTests.get(1).getRequirements(emptyConfiguration()), DummyRequirement2.class)).isFalse();
    }

    @Test
    public void shouldCreateConventionTestWithWrongRequires()
            throws IOException
    {
        Path testPath = getPathForConventionTest("-- requires: not.existing.Requirement");

        assertThatThrownBy(() -> sqlPathTestFactory.createTestsForPath(testPath, "", null))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Unable to find specified class: not.existing.Requirement");
    }

    @Test
    public void shouldCreateTestWhenNoResultsFile()
            throws IOException
    {
        Path testPath = getPathForConventionTest("--", Optional.empty());

        List<ConventionBasedTest> tests = sqlPathTestFactory.createTestsForPath(testPath, "", null);

        assertThat(tests).hasSize(1);
    }

    @Test
    public void shouldFailInvalidNumberOfSections()
            throws IOException
    {
        Path testPath = getPathForConventionTest("--\n--!", Optional.empty());

        assertThatThrownBy(() -> sqlPathTestFactory.createTestsForPath(testPath, "", null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("First section should contain properties, next sections should represent query and results");
    }

    private Path getPathForConventionTest(String conventionTestContent)
            throws IOException
    {
        return getPathForConventionTest(conventionTestContent, Optional.of(""));
    }

    private Path getPathForConventionTest(String conventionTestContent, Optional<String> resultFileContent)
            throws IOException
    {
        Path file = temporaryFolder.resolve(UUID.randomUUID().toString() + ".tmp");
        Files.writeString(file, conventionTestContent, StandardCharsets.UTF_8);
        Path testPath = Paths.get(file.toString());

        if (resultFileContent.isPresent()) {
            Path resultFile = Paths.get(testPath.toString().replace(".tmp", ".result"));
            Files.writeString(resultFile, resultFileContent.get(), StandardCharsets.UTF_8);
        }

        return testPath;
    }

    private boolean containsRequirement(Requirement requirement, Class<? extends Requirement> requirementClass)
    {
        if (requirement instanceof CompositeRequirement compositeRequirement) {
            return compositeRequirement.getRequirementsSets().stream()
                    .anyMatch(set -> set.stream()
                            .anyMatch(element -> containsRequirement(element, requirementClass)));
        }
        else {
            return requirementClass.isInstance(requirement);
        }
    }

    public static class DummyRequirementsProvider1
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return new DummyRequirement1();
        }
    }

    public static class DummyRequirementsProvider2
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return new DummyRequirement2();
        }
    }

    public static class DummyRequirement1
            implements Requirement {}

    public static class DummyRequirement2
            implements Requirement {}
}
