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

package io.trino.tempto.internal.fulfillment.table;

import io.trino.tempto.fulfillment.table.TableDefinition;
import io.trino.tempto.fulfillment.table.TableHandle;
import io.trino.tempto.fulfillment.table.TableManager;
import io.trino.tempto.fulfillment.table.hive.HiveDataSource;
import io.trino.tempto.fulfillment.table.hive.HiveTableDefinition;
import io.trino.tempto.fulfillment.table.jdbc.RelationalDataSource;
import io.trino.tempto.fulfillment.table.jdbc.RelationalTableDefinition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static io.trino.tempto.fulfillment.table.TableHandle.tableHandle;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultTableManagerDispatcherTest
{
    private TableManager hiveTableManager1;
    private TableManager psqlTableManager1;
    private TableManager psqlTableManager2;
    private DefaultTableManagerDispatcher instance;
    private Map<String, TableManager> tableManagers;

    @BeforeEach
    public void setup()
    {
        hiveTableManager1 = mock(TableManager.class);
        lenient().when(hiveTableManager1.getTableDefinitionClass()).thenReturn(HiveTableDefinition.class);
        lenient().when(hiveTableManager1.getDatabaseName()).thenReturn("hive1");

        psqlTableManager1 = mock(TableManager.class);
        lenient().when(psqlTableManager1.getTableDefinitionClass()).thenReturn(RelationalTableDefinition.class);
        lenient().when(psqlTableManager1.getDatabaseName()).thenReturn("psql1");

        psqlTableManager2 = mock(TableManager.class);
        lenient().when(psqlTableManager2.getTableDefinitionClass()).thenReturn(RelationalTableDefinition.class);
        lenient().when(psqlTableManager2.getDatabaseName()).thenReturn("psql2");

        tableManagers = new HashMap<>();
        tableManagers.put("hive1", hiveTableManager1);
        tableManagers.put("psql1", psqlTableManager1);
        tableManagers.put("psql2", psqlTableManager2);
        instance = new DefaultTableManagerDispatcher(tableManagers);
    }

    @ParameterizedTest
    @MethodSource("getTableManagerForData")
    public void testGetTableMangerFor(TableDefinition definitionClass, TableHandle tableHandle, String tableManager)
    {
        assertThat(instance.getTableManagerFor(definitionClass, tableHandle)).isEqualTo(tableManagers.get(tableManager));
    }

    static Stream<Arguments> getTableManagerForData()
    {
        return Stream.of(
                Arguments.of(hiveTableDefinition(), hiveTableDefinition().getTableHandle(), "hive1"),
                Arguments.of(hiveTableDefinition(), hiveTableDefinition().getTableHandle().inDatabase("hive1"), "hive1"),
                Arguments.of(jdbcTableDefinition(), jdbcTableDefinition().getTableHandle().inDatabase("psql1"), "psql1"),
                Arguments.of(jdbcTableDefinition(), jdbcTableDefinition().getTableHandle().inDatabase("psql2"), "psql2"));
    }

    @Test
    public void multipleDatabasesForTableDefinitionClass()
    {
        assertThatThrownBy(() -> instance.getTableManagerFor(jdbcTableDefinition()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Multiple databases found for table: TableHandle{name=name}, definition class 'class io.trino.tempto.fulfillment.table.jdbc.RelationalTableDefinition'. Pick a database from [psql1, psql2]");
    }

    @Test
    public void noDatabaseFound()
    {
        assertThatThrownBy(() -> instance.getTableManagerFor(jdbcTableDefinition(), jdbcTableDefinition().getTableHandle().inDatabase("unknown")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No table manager found for table: TableHandle{database=unknown, name=name}");
    }

    @Test
    public void wrongTableDefinition()
    {
        assertThatThrownBy(() -> instance.getTableManagerFor(jdbcTableDefinition(), jdbcTableDefinition().getTableHandle().inDatabase("hive1")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("does not match requested table definition class");
    }

    private static RelationalTableDefinition jdbcTableDefinition()
    {
        return RelationalTableDefinition.relationalTableDefinition(tableHandle("name"), "ddl %NAME% %LOCATION%", mock(RelationalDataSource.class));
    }

    private static HiveTableDefinition hiveTableDefinition()
    {
        return HiveTableDefinition.hiveTableDefinition(tableHandle("name"), "ddl %NAME% %LOCATION%", mock(HiveDataSource.class));
    }
}
