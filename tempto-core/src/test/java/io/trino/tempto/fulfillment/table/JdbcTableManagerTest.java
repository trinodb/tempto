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

package io.trino.tempto.fulfillment.table;

import io.trino.tempto.fulfillment.table.jdbc.RelationalDataSource;
import io.trino.tempto.fulfillment.table.jdbc.RelationalTableDefinition;
import io.trino.tempto.internal.configuration.EmptyConfiguration;
import io.trino.tempto.internal.fulfillment.table.TableNameGenerator;
import io.trino.tempto.internal.fulfillment.table.jdbc.JdbcTableManager;
import io.trino.tempto.query.QueryExecutor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

public class JdbcTableManagerTest
{
    private RelationalTableDefinition tableDefinition;
    private JdbcTableManager tableManager;
    private String tableName;

    @BeforeEach
    public void setup()
            throws Exception
    {
        tableName = "name";
        RelationalDataSource dataSource = () -> Collections.<List<Object>>emptyList().iterator();
        tableDefinition = RelationalTableDefinition.relationalTableDefinition(tableName, "CREATE TABLE %NAME%(col1 INT)", dataSource);

        QueryExecutor mockExecutor = mock(QueryExecutor.class);
        Connection mockConnection = mock(Connection.class);
        DatabaseMetaData mockMetadata = mock(DatabaseMetaData.class);
        lenient().when(mockExecutor.getConnection()).thenReturn(mockConnection);
        lenient().when(mockConnection.getMetaData()).thenReturn(mockMetadata);

        ResultSet mockResultSet = mock(ResultSet.class);
        lenient().when(mockMetadata.getTables(any(), any(), any(), any(String[].class))).thenReturn(mockResultSet);
        lenient().when(mockResultSet.getMetaData()).thenReturn(mock(ResultSetMetaData.class));
        tableManager = new JdbcTableManager(mockExecutor, new TableNameGenerator(), "db_name", EmptyConfiguration.emptyConfiguration());
    }

    @Test
    public void tableWithoutRowsDoesNotThrow()
    {
        assertThatCode(() -> tableManager.createImmutable(tableDefinition, TableHandle.tableHandle(tableName)))
                .doesNotThrowAnyException();
    }
}
