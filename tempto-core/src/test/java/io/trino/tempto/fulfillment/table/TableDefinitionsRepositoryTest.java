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

import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.List;

import static io.trino.tempto.fulfillment.table.TableHandle.tableHandle;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TableDefinitionsRepositoryTest
{
    @Test
    public void shouldAddGetTableDefinitionToRepository()
            throws Exception
    {
        TableDefinition tpchCustomer = mock(TableDefinition.class);
        when(tpchCustomer.getTableHandle()).thenReturn(tableHandle("customer"));
        TableDefinition tpcdsCustomer = mock(TableDefinition.class);
        when(tpcdsCustomer.getTableHandle()).thenReturn(tableHandle("customer").inSchema("tpcds"));
        TableDefinition noSchemaSample = mock(TableDefinition.class);
        when(noSchemaSample.getTableHandle()).thenReturn(tableHandle("noSchemaSample"));
        TableDefinition sampleInSchema = mock(TableDefinition.class);
        when(sampleInSchema.getTableHandle()).thenReturn(tableHandle("sample").inSchema("schema"));

        TableDefinitionsRepository repository = newRepository(
                List.of(tpchCustomer, tpcdsCustomer, noSchemaSample, sampleInSchema));

        assertThat(repository.get(tableHandle("noSchemaSample"))).isEqualTo(noSchemaSample);
        assertThat(repository.get(tableHandle("sample").inSchema("schema"))).isEqualTo(sampleInSchema);
        assertThat(repository.get(tableHandle("customer"))).isEqualTo(tpchCustomer);
        assertThat(repository.get(tableHandle("customer").inSchema("tpcds"))).isEqualTo(tpcdsCustomer);
        assertThat(repository.get(tableHandle("noSchemaSample").inSchema("tpcds"))).isEqualTo(noSchemaSample);

        assertThatThrownBy(() -> repository.get(tableHandle("sample").inSchema("tpcds")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("no table definition for: tpcds.sample");

        assertThatThrownBy(() -> repository.get(tableHandle("sample")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("no table definition for: sample");
    }

    private static TableDefinitionsRepository newRepository(Collection<TableDefinition> tableDefinitions)
            throws Exception
    {
        Constructor<TableDefinitionsRepository> constructor =
                TableDefinitionsRepository.class.getDeclaredConstructor(Collection.class);
        constructor.setAccessible(true);
        return constructor.newInstance(tableDefinitions);
    }
}
