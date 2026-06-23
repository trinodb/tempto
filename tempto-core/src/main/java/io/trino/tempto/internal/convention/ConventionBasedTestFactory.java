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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.internal.convention.generator.GeneratorPathTestFactory;
import io.trino.tempto.internal.convention.recursion.RecursionPathTestFactory;
import io.trino.tempto.internal.convention.sql.SqlPathTestFactory;
import io.trino.tempto.internal.listeners.TestNameGroupNameFilter;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.tempto.fulfillment.table.TableDefinitionsRepository.tableDefinitionsRepository;
import static io.trino.tempto.internal.configuration.TestConfigurationFactory.testConfiguration;
import static io.trino.tempto.internal.convention.ConventionTestsUtils.getConventionsTestsPath;
import static java.util.stream.Collectors.toList;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Discovers convention-based (file driven) tests from the {@code testcases} directory tree.
 */
public class ConventionBasedTestFactory
{
    private static final Logger LOGGER = getLogger(ConventionBasedTestFactory.class);

    public static final String TESTCASES_PATH_PART = "testcases";

    // Discovery walks the testcases tree and parses every file; do it once per JVM and share between
    // suite-level requirement resolution and @TestTemplate expansion.
    private static final Supplier<List<ConventionBasedTest>> CONVENTION_TESTS =
            Suppliers.memoize(() -> new ConventionBasedTestFactory().createTestCases());

    public interface PathTestFactory
    {
        boolean isSupportedPath(Path path);

        List<ConventionBasedTest> createTestsForPath(Path path, String testNamePrefix, ConventionBasedTestFactory factory);
    }

    private final List<PathTestFactory> factories = setupFactories();

    /**
     * The discovered convention tests that match the given name/group selection. Discovery is cached;
     * both the suite-level requirement resolver and the {@code @TestTemplate} provider call this so
     * they always agree on which tests run.
     */
    public static List<ConventionBasedTest> selectedConventionTests(TestNameGroupNameFilter filter)
    {
        return CONVENTION_TESTS.get().stream()
                .filter(test -> filter.matches(test.getTestName(), test.getTestGroups()))
                .collect(toImmutableList());
    }

    public List<ConventionBasedTest> createTestCases()
    {
        LOGGER.debug("Loading file based test cases");
        try {
            Optional<Path> productTestsPath = getConventionsTestsPath(TESTCASES_PATH_PART);
            if (productTestsPath.isEmpty()) {
                LOGGER.info("No convention tests cases");
                return ImmutableList.of();
            }

            return createTestsForPath(productTestsPath.get(), "sql_tests");
        }
        catch (Exception e) {
            LOGGER.error("Could not create file test", e);
            throw new RuntimeException("Could not create test cases", e);
        }
    }

    private List<PathTestFactory> setupFactories()
    {
        Configuration configuration = testConfiguration();
        return ImmutableList.of(
                new RecursionPathTestFactory(),
                new GeneratorPathTestFactory(),
                new SqlPathTestFactory(
                        tableDefinitionsRepository(),
                        configuration));
    }

    public List<ConventionBasedTest> createTestsForPath(Path path, String testNamePrefix)
    {
        return factories.stream()
                .filter(f -> f.isSupportedPath(path))
                .flatMap(f -> f.createTestsForPath(path, testNamePrefix, this).stream())
                .collect(toList());
    }

    public List<ConventionBasedTest> createTestsForChildrenOfPath(Path path, String testNamePrefix)
    {
        try {
            // TODO tree traversal for ZIP file system (when resources are inside jar) results with Exception
            return Files.list(path)
                    .flatMap(child -> createTestsForPath(child, testNamePrefix).stream())
                    .collect(toList());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
