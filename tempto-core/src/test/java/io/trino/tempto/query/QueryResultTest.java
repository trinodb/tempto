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

import org.junit.jupiter.api.Test;

import java.sql.JDBCType;
import java.sql.ResultSet;
import java.util.List;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

public class QueryResultTest
{
    @Test
    public void testQueryResultProject()
    {
        ResultSet jdbcResultSet = mock(ResultSet.class);
        List<JDBCType> columnTypes = asList(JDBCType.CHAR, JDBCType.DOUBLE, JDBCType.INTEGER);
        List<String> columnNames = asList("char", "double", "integer");
        QueryResult.QueryResultBuilder builder = new QueryResult.QueryResultBuilder(columnTypes, columnNames);
        builder.addRow("aaa", 1.0, 1);
        builder.addRow("bbb", 2.0, 2);
        builder.setJdbcResultSet(jdbcResultSet);
        QueryResult queryResult = builder.build();
        QueryResult projection = queryResult.project(1, 3);

        assertThat(projection.rows()).isEqualTo(asList(asList("aaa", 1), asList("bbb", 2)));
        assertThat(projection.getColumnsCount()).isEqualTo(2);
        assertThat(projection.column(1)).isEqualTo(asList("aaa", "bbb"));
        assertThat(projection.column(2)).isEqualTo(asList(1, 2));
        assertThat(projection.getJdbcResultSet().get()).isEqualTo(jdbcResultSet);
    }

    @Test
    public void testQueryResultOnlyValue()
    {
        ResultSet jdbcResultSet = mock(ResultSet.class);
        List<JDBCType> columnTypes = asList(JDBCType.VARCHAR);
        List<String> columnNames = asList("varchar");
        QueryResult.QueryResultBuilder builder = new QueryResult.QueryResultBuilder(columnTypes, columnNames);
        builder.addRow("tempto");
        builder.setJdbcResultSet(jdbcResultSet);
        QueryResult queryResult = builder.build();

        assertThat(queryResult.getOnlyValue()).isEqualTo("tempto");
    }

    @Test
    public void testQueryResultOnlyValueWithMultipleRows()
    {
        ResultSet jdbcResultSet = mock(ResultSet.class);
        List<JDBCType> columnTypes = asList(JDBCType.VARCHAR);
        List<String> columnNames = asList("varchar");
        QueryResult.QueryResultBuilder builder = new QueryResult.QueryResultBuilder(columnTypes, columnNames);
        builder.addRow("tempto");
        builder.addRow("tempto");
        builder.setJdbcResultSet(jdbcResultSet);
        QueryResult multiRowQueryResult = builder.build();

        assertThatThrownBy(multiRowQueryResult::getOnlyValue)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testQueryResultOnlyValueWithMultipleColumns()
    {
        ResultSet jdbcResultSet = mock(ResultSet.class);
        List<JDBCType> columnTypes = asList(JDBCType.VARCHAR, JDBCType.VARCHAR);
        List<String> columnNames = asList("varchar", "varchar");
        QueryResult.QueryResultBuilder builder = new QueryResult.QueryResultBuilder(columnTypes, columnNames);
        builder.addRow("aaa", "bbb");
        builder.setJdbcResultSet(jdbcResultSet);
        QueryResult multiColumnQueryResult = builder.build();

        assertThatThrownBy(multiColumnQueryResult::getOnlyValue)
                .isInstanceOf(IllegalStateException.class);
    }
}
