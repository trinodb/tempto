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

package io.trino.tempto.examples;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.Requirement;
import io.trino.tempto.RequirementsProvider;
import io.trino.tempto.Requires;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.fulfillment.table.ImmutableTableRequirement;
import io.trino.tempto.fulfillment.table.MutableTablesState;
import io.trino.tempto.fulfillment.table.TableDefinition;
import io.trino.tempto.fulfillment.table.TableInstance;
import io.trino.tempto.fulfillment.table.TableManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static io.trino.tempto.Requirements.compose;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.context.ThreadLocalTestContextHolder.testContext;
import static io.trino.tempto.context.ThreadLocalTestContextHolder.testContextIfSet;
import static io.trino.tempto.fulfillment.table.MutableTableRequirement.State.CREATED;
import static io.trino.tempto.fulfillment.table.MutableTableRequirement.State.LOADED;
import static io.trino.tempto.fulfillment.table.TableRequirements.mutableTable;
import static io.trino.tempto.fulfillment.table.hive.HiveTableDefinition.like;
import static io.trino.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.NATION;
import static io.trino.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.REGION;
import static io.trino.tempto.query.QueryExecutor.query;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class SimpleQueryTest
        extends ProductTest
{
    private static class SimpleTestRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return new ImmutableTableRequirement(NATION);
        }
    }

    @Inject
    @Named("hive")
    TableManager tableManager;

    @BeforeEach
    public void someBefore()
    {
        // just to check if having @BeforeEach method does not break anything
    }

    @AfterEach
    public void someAfter()
    {
        // just to check if having @AfterEach method does not break anything
    }

    @BeforeMethodWithContext
    public void beforeTest()
    {
        assertThat(testContextIfSet().isPresent()).isTrue();
    }

    @AfterMethodWithContext
    public void afterTest()
    {
        assertThat(testContextIfSet().isPresent()).isTrue();
    }

    @Test
    @Tag("query")
    @Timeout(value = 1000000, unit = MILLISECONDS)
    public void createAndDropMutableTable()
    {
        TableDefinition tableDefinition = like(NATION)
                .setNoData()
                .setName("some_other_table_name")
                .build();

        tableManager.createMutable(tableDefinition, CREATED);
        tableManager.createMutable(tableDefinition);
    }

    @Test
    @Tag("query")
    @Requires(SimpleTestRequirements.class)
    public void selectAllFromNation()
    {
        assertThat(query("select * from nation")).hasRowsCount(25);
    }

    @Test
    @Tag("smoke")
    @Tag("query")
    @Requires(SimpleTestRequirements.class)
    public void selectCountFromNation()
    {
        assertThat(query("select count(*) from nation"))
                .hasRowsCount(1)
                .contains(row(25));
    }

    private static class MultipleTablesTestRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            // Native JUnit does not expand a single test into multiple instances, so unlike the
            // former Requirements.allOf(...) this fulfills both tables for one test run; the tables
            // therefore need distinct names.
            return compose(
                    mutableTable(NATION, "table", LOADED),
                    mutableTable(REGION, "table_region", LOADED));
        }
    }

    @Test
    @Tag("query")
    @Requires(MultipleTablesTestRequirements.class)
    public void selectAllFromMultipleTables()
    {
        MutableTablesState mutableTablesState = testContext().getDependency(MutableTablesState.class);
        TableInstance tableInstance = mutableTablesState.get("table");
        assertThat(query("select * from " + tableInstance.getNameInDatabase())).hasAnyRows();
    }

    @Test
    @Tag("failing")
    public void failingTest()
    {
        assertThat(1).isEqualTo(2);
    }

    @Test
    @Tag("skipped")
    @Disabled
    public void disabledTest()
    {
        assertThat(1).isEqualTo(2);
    }
}
