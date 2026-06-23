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

import io.trino.tempto.internal.fulfillment.table.TableName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static io.trino.tempto.fulfillment.table.TableHandle.tableHandle;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TablesStateTest
{
    private static final TableInstance db1_table1 = table("db1", "table1");

    private static final TableInstance db1_table2 = table("db1", "table2");
    private static final TableInstance db1_schema1_table2 = table("db1", "schema1", "table2");

    private static final TableInstance db1_table3 = table("db1", "table3");
    private static final TableInstance db2_table3 = table("db2", "table3");

    private static final TableInstance db1_schema2_table4 = table("db1", "schema2", "table4");
    private static final TableInstance db2_schema2_table4 = table("db2", "schema2", "table4");

    private static final TableInstance db1_table5 = table("db1", "table5");
    private static final TableInstance db1_schema3_table5 = table("db1", "schema3", "table5");

    private static final TableInstance db1_schema4_table6 = table("db1", "schema4", "table6");
    private static final TableInstance db1_schema5_table6 = table("db1", "schema5", "table6");

    private static final TableInstance db1_schema6_table7 = table("db1", "schema6", "table7");

    private TablesState tablesState;

    @BeforeEach
    public void setup()
    {
        tablesState = new TablesState(
                List.of(
                        db1_table1,
                        db1_table2, db1_schema1_table2,
                        db1_table3, db2_table3,
                        db1_schema2_table4, db2_schema2_table4,
                        db1_table5, db1_schema3_table5,
                        db1_schema4_table6, db1_schema5_table6,
                        db1_schema6_table7),
                "table") {};
    }

    @ParameterizedTest
    @MethodSource("findDataProvider")
    public void find(TableHandle tableHandle, TableInstance expectedTable)
    {
        assertThat(tablesState.get(tableHandle)).isEqualTo(expectedTable);
    }

    static Stream<Arguments> findDataProvider()
    {
        return Stream.of(
                Arguments.of(tableHandle("table1"), db1_table1),
                Arguments.of(tableHandle("table2").inSchema("schema1"), db1_schema1_table2),
                Arguments.of(tableHandle("table3").inDatabase("db1"), db1_table3),
                Arguments.of(tableHandle("table4").inDatabase("db1").inSchema("schema2"), db1_schema2_table4),
                Arguments.of(tableHandle("table5").inDatabase("db1").inSchema("schema3"), db1_schema3_table5),
                Arguments.of(tableHandle("table5").withNoSchema(), db1_table5),
                Arguments.of(tableHandle("table6").inSchema("schema5"), db1_schema5_table6),
                Arguments.of(tableHandle("table7"), db1_schema6_table7));
    }

    @Test
    public void noTableFound()
    {
        assertThatThrownBy(() -> tablesState.get(tableHandle("unknown")))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("No table instance found");
    }

    @Test
    public void multipleTablesFound()
    {
        assertThatThrownBy(() -> tablesState.get(tableHandle("table3")))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Multiple table instances found");
    }

    private static TableInstance table(String... names)
    {
        TableName name;
        if (names.length == 2) {
            name = new TableName(names[0], Optional.empty(), names[1], names[1]);
        }
        else if (names.length == 3) {
            name = new TableName(names[0], Optional.of(names[1]), names[2], names[2]);
        }
        else {
            throw new IllegalArgumentException("Unexpected number of names: " + names.length);
        }
        return new TableInstance(name, new TableDefinition(tableHandle("ignore")) {});
    }
}
