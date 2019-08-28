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
package io.prestosql.tempto.examples;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import io.prestosql.tempto.ProductTest;
import io.prestosql.tempto.Requirement;
import io.prestosql.tempto.Requirements;
import io.prestosql.tempto.RequirementsProvider;
import io.prestosql.tempto.Requires;
import io.prestosql.tempto.configuration.Configuration;
import io.prestosql.tempto.fulfillment.table.ImmutableTableRequirement;
import io.prestosql.tempto.fulfillment.table.MutableTableRequirement;
import io.prestosql.tempto.fulfillment.table.MutableTablesState;
import io.prestosql.tempto.fulfillment.table.TableInstance;
import io.prestosql.tempto.fulfillment.table.jdbc.RelationalDataSource;
import io.prestosql.tempto.fulfillment.table.jdbc.RelationalTableDefinition;
import io.prestosql.tempto.query.QueryExecutor;
import io.prestosql.tempto.query.QueryResult;
import org.testng.annotations.Test;

import javax.inject.Named;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tempto.fulfillment.table.ImmutableTablesState.immutableTablesState;
import static io.prestosql.tempto.fulfillment.table.TableHandle.tableHandle;
import static io.prestosql.tempto.fulfillment.table.jdbc.RelationalTableDefinition.relationalTableDefinition;

public class PostgresqlQueryTest
        extends ProductTest
{
    @Inject
    @Named("psql")
    private QueryExecutor queryExecutor;

    @Inject
    private MutableTablesState mutableTablesState;

    private static final RelationalTableDefinition TEST_TABLE_DEFINITION;

    static {
        RelationalDataSource dataSource = () -> ImmutableList.<List<Object>>of(
                ImmutableList.of(1, "x"),
                ImmutableList.of(2, "y")
        ).iterator();
        TEST_TABLE_DEFINITION = relationalTableDefinition("test_table", "CREATE TABLE %NAME% (a int, b varchar(100))", dataSource);
    }

    private static class ImmutableTestJdbcTables
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return Requirements.compose(
                    new ImmutableTableRequirement(RelationalTableDefinition.like(TEST_TABLE_DEFINITION).withDatabase("psql").build()),
                    new ImmutableTableRequirement(RelationalTableDefinition.like(TEST_TABLE_DEFINITION).withDatabase("psql").withSchema("test_schema").build())
            );
        }
    }

    private static class MutableTestJdbcTables
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return Requirements.compose(
                    MutableTableRequirement.builder(TEST_TABLE_DEFINITION).withDatabase("psql").build(),
                    MutableTableRequirement.builder(TEST_TABLE_DEFINITION).withDatabase("psql").withSchema("test_schema").build()
            );
        }
    }

    @Test(groups = "psql_query")
    @Requires(ImmutableTestJdbcTables.class)
    public void selectFromImmutableTable()
    {
        String nameInDatabase = immutableTablesState().get(tableHandle("test_table").withNoSchema()).getNameInDatabase();
        assertThat(queryExecutor.executeQuery("select * from " + nameInDatabase)).containsOnly(row(1, "x"), row(2, "y"));
    }

    @Test(groups = "psql_query")
    @Requires(MutableTestJdbcTables.class)
    public void selectFromMutableTable()
    {
        String tableName = mutableTablesState.get(tableHandle("test_table").withNoSchema()).getNameInDatabase();
        assertThat(queryExecutor.executeQuery("select * from " + tableName)).containsOnly(row(1, "x"), row(2, "y"));
    }

    @Test(groups = {"psql_query"})
    @Requires(MutableTestJdbcTables.class)
    public void selectFromTableInDifferentSchema()
    {
        TableInstance tableInstance = mutableTablesState.get(tableHandle("test_table").inSchema("test_schema"));
        assertThat(queryExecutor.executeQuery("select * from " + tableInstance.getNameInDatabase())).containsOnly(row(1, "x"), row(2, "y"));
    }

    @Test(groups = {"psql_query"})
    public void useNewConnection()
            throws SQLException
    {
        String defaultTimeZone = Iterables.getOnlyElement(queryExecutor.executeQuery("SHOW TIME ZONE").column(1));
        checkState(!defaultTimeZone.toLowerCase(Locale.ENGLISH).contains("/rome"), "Test assumes default zone is not Europe/Rome");

        queryExecutor.inNewConnection(connection -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute("SET TIME ZONE 'Europe/Rome'");
            }
            try (Statement statement = connection.createStatement()) {
                assertThat(QueryResult.forResultSet(statement.executeQuery("SHOW TIME ZONE")))
                        .containsOnly(row("Europe/Rome"));
            }
            return null;
        });

        // Verify default time zone remained intact
        assertThat(queryExecutor.executeQuery("SHOW TIME ZONE"))
                .containsOnly(row(defaultTimeZone));
    }
}
