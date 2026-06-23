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
package io.trino.tempto.internal.convention;

import io.trino.tempto.internal.convention.AnnotatedFileParser.SectionParsingResult;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.tempto.fulfillment.table.MutableTableRequirement.State.CREATED;
import static io.trino.tempto.fulfillment.table.MutableTableRequirement.State.LOADED;
import static io.trino.tempto.fulfillment.table.TableHandle.tableHandle;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.io.IOUtils.toInputStream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlQueryDescriptorTest
{
    @Test
    public void parsesImmutableTableProperties()
            throws IOException
    {
        String fileContent = "-- tables: table1, schema.table2, db.schema.table3";
        SectionParsingResult parsingResult = parseSection(fileContent);
        SqlQueryDescriptor queryDescriptor = new SqlQueryDescriptor(parsingResult);

        assertThat(queryDescriptor.getTableDefinitionHandles()).containsExactlyInAnyOrder(
                tableHandle("table1"),
                tableHandle("table2").inSchema("schema"),
                tableHandle("table3").inSchema("schema").inDatabase("db"));
    }

    @Test
    public void parsesMutableTableProperties()
            throws IOException
    {
        String fileContent = "-- mutable_tables: table1|loaded|table1_name, table2|created, table3, table4|created|prefix.table4_1";
        SectionParsingResult parsingResult = parseSection(fileContent);
        SqlQueryDescriptor queryDescriptor = new SqlQueryDescriptor(parsingResult);

        assertThat(queryDescriptor.getMutableTableDescriptors()).hasSize(4);

        assertThat(queryDescriptor.getMutableTableDescriptors().get(0).tableDefinitionName).isEqualTo("table1");
        assertThat(queryDescriptor.getMutableTableDescriptors().get(0).state).isEqualTo(LOADED);
        assertThat(queryDescriptor.getMutableTableDescriptors().get(0).tableHandle).isEqualTo(tableHandle("table1_name"));

        assertThat(queryDescriptor.getMutableTableDescriptors().get(1).tableDefinitionName).isEqualTo("table2");
        assertThat(queryDescriptor.getMutableTableDescriptors().get(1).state).isEqualTo(CREATED);
        assertThat(queryDescriptor.getMutableTableDescriptors().get(1).tableHandle).isEqualTo(tableHandle("table2"));

        assertThat(queryDescriptor.getMutableTableDescriptors().get(2).tableDefinitionName).isEqualTo("table3");
        assertThat(queryDescriptor.getMutableTableDescriptors().get(2).state).isEqualTo(LOADED);
        assertThat(queryDescriptor.getMutableTableDescriptors().get(2).tableHandle).isEqualTo(tableHandle("table3"));

        assertThat(queryDescriptor.getMutableTableDescriptors().get(3).tableDefinitionName).isEqualTo("table4");
        assertThat(queryDescriptor.getMutableTableDescriptors().get(3).state).isEqualTo(CREATED);
        assertThat(queryDescriptor.getMutableTableDescriptors().get(3).tableHandle).isEqualTo(tableHandle("table4_1").inSchema("prefix"));
    }

    @Test
    public void shouldFailDuplicateMutableTableName()
            throws IOException
    {
        String fileContent = "-- mutable_tables: table1, table1";
        SectionParsingResult parsingResult = parseSection(fileContent);
        SqlQueryDescriptor queryDescriptor = new SqlQueryDescriptor(parsingResult);

        assertThatThrownBy(queryDescriptor::getMutableTableDescriptors)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Table with name table1 is defined twice");
    }

    private SectionParsingResult parseSection(String content)
            throws IOException
    {
        return getOnlyElement(new AnnotatedFileParser().parseFile(toInputStream(content, UTF_8)));
    }
}
