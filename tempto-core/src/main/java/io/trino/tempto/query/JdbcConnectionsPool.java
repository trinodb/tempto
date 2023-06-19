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
package io.trino.tempto.query;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static io.trino.tempto.internal.query.JdbcUtils.dataSource;
import static java.util.Objects.requireNonNull;

public class JdbcConnectionsPool
{
    private final Map<JdbcConnectivityParamsState, DataSource> dataSources = newHashMap();

    public Connection connectionFor(JdbcConnectivityParamsState jdbcParamsState)
            throws SQLException
    {
        return configureConnection(jdbcParamsState, createConnection(jdbcParamsState));
    }

    protected Connection createConnection(JdbcConnectivityParamsState jdbcParamsState)
            throws SQLException
    {
        if (!dataSources.containsKey(jdbcParamsState)) {
            dataSources.put(jdbcParamsState, dataSource(jdbcParamsState));
        }

        Connection connection = dataSources.get(jdbcParamsState).getConnection();
        if (connection == null) {
            // this should never happen, `javax.sql.DataSource#getConnection()` should not return null
            throw new IllegalStateException("No connection was created for: " + jdbcParamsState.getName());
        }
        return connection;
    }

    protected static Connection configureConnection(JdbcConnectivityParamsState jdbcParamsState, Connection connection)
            throws SQLException
    {
        requireNonNull(connection, "connection is null");
        if (!jdbcParamsState.prepareStatements.isEmpty()) {
            try (Statement statement = connection.createStatement()) {
                for (String query : jdbcParamsState.prepareStatements) {
                    try {
                        statement.execute(query);
                    }
                    catch (SQLException e) {
                        throw new SQLException("Preparatory statement failed: " + query, e);
                    }
                }
            }
        }
        return connection;
    }
}
