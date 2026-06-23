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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static io.trino.tempto.fulfillment.table.TableHandle.tableHandle;
import static org.assertj.core.api.Assertions.assertThat;

public class TableHandleTest
{
    @ParameterizedTest
    @MethodSource("parseDataProvider")
    public void parse(String tableHandleStr, TableHandle expectedTableHandle)
    {
        assertThat(TableHandle.parse(tableHandleStr)).isEqualTo(expectedTableHandle);
    }

    static Stream<Arguments> parseDataProvider()
    {
        return Stream.of(
                Arguments.of("table", tableHandle("table")),
                Arguments.of("schema.table", tableHandle("table").inSchema("schema")),
                Arguments.of("db.schema.table", tableHandle("table").inDatabase("db").inSchema("schema")));
    }
}
