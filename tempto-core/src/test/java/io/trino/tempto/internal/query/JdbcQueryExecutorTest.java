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

package io.trino.tempto.internal.query;

import io.trino.tempto.context.TestContext;
import io.trino.tempto.internal.context.GuiceTestContext;
import io.trino.tempto.query.JdbcConnectionsPool;
import io.trino.tempto.query.JdbcConnectivityParamsState;
import io.trino.tempto.query.JdbcQueryExecutor;
import io.trino.tempto.query.QueryResult;
import org.apache.commons.dbutils.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.internal.configuration.TestConfigurationFactory.TEST_CONFIGURATION_URIS_KEY;
import static io.trino.tempto.internal.query.JdbcUtils.connection;
import static io.trino.tempto.internal.query.JdbcUtils.registerDriver;
import static java.sql.JDBCType.INTEGER;
import static java.sql.JDBCType.VARCHAR;

public class JdbcQueryExecutorTest
{
    private static final JdbcConnectivityParamsState JDBC_STATE =
            JdbcConnectivityParamsState.builder()
                    .setName("connection_name")
                    .setDriverClass("org.hsqldb.jdbc.JDBCDriver")
                    .setUrl("jdbc:hsqldb:mem:mydb")
                    .setUser("sa")
                    .setPooling(true)
                    .build();

    private static TestContext testContext = new GuiceTestContext();
    private final JdbcQueryExecutor queryExecutor = new JdbcQueryExecutor(JDBC_STATE, new JdbcConnectionsPool(), testContext);

    @BeforeAll
    public static void setupSpec()
    {
        System.setProperty(TEST_CONFIGURATION_URIS_KEY, "/configuration/global-configuration-tempto.yaml");
        registerDriver(JDBC_STATE);
    }

    @AfterAll
    public static void cleanupSpec()
    {
        testContext.close();
    }

    @BeforeEach
    public void setup()
            throws SQLException
    {
        Connection c = null;
        QueryRunner run = new QueryRunner();
        try {
            c = connection(JDBC_STATE);
            run.update(c, "DROP SCHEMA PUBLIC CASCADE");
            run.update(c,
                    "CREATE TABLE  company ( "
                            + "comp_name varchar(100) NOT NULL, "
                            + "comp_id int "
                            + ")");
            run.update(c, "INSERT INTO company(comp_id, comp_name) values (1, 'Teradata')");
            run.update(c, "INSERT INTO company(comp_id, comp_name) values (2, 'Oracle')");
            run.update(c, "INSERT INTO company(comp_id, comp_name) values (3, 'Starburst')");
        }
        finally {
            if (c != null) {
                c.close();
            }
        }
    }

    @Test
    public void testSelect()
    {
        QueryResult result = queryExecutor.executeQuery("SELECT comp_id, comp_name FROM company ORDER BY comp_id");

        assertThat(result)
                .hasColumns(INTEGER, VARCHAR)
                .containsExactly(
                        row(1, "Teradata"),
                        row(2, "Oracle"),
                        row(3, "Starburst"));
    }

    @Test
    public void testUpdate()
    {
        QueryResult result = queryExecutor.executeQuery("UPDATE company SET comp_name='Starburst Data' WHERE comp_id=3");

        assertThat(result)
                .hasColumns(INTEGER)
                .containsExactly(
                        row(1));

        result = queryExecutor.executeQuery("SELECT comp_id, comp_name FROM company ORDER BY comp_id");

        assertThat(result)
                .hasColumns(INTEGER, VARCHAR)
                .containsExactly(
                        row(1, "Teradata"),
                        row(2, "Oracle"),
                        row(3, "Starburst Data"));
    }
}
