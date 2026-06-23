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
import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.io.IOUtils.toInputStream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class AnnotatedFileParserTest
{
    private final AnnotatedFileParser fileParser = new AnnotatedFileParser();

    @Test
    public void parseFileWithCommentsPropertiesAndWhitespaceLines()
            throws IOException
    {
        String fileContent = "-- property1: value1;\n" +
                "-- property2: value2\n" +
                "content line 1\n" +
                "--- comment line\n" +
                "  \n" +  // whitespace line
                "--- property3: value3\n" +
                "content line 2\n" +
                "\\--- contentproperty: x\n" +
                "\\# content comment";
        SectionParsingResult parsingResult = parseOnlySection(fileContent);

        assertThat(parsingResult.getProperty("property1")).hasValue("value1");
        assertThat(parsingResult.getProperty("property2")).hasValue("value2");
        assertThat(parsingResult.getProperty("property3")).isNotPresent();
        assertThat(parsingResult.getProperty("unknownProperty")).isNotPresent();
        assertThat(parsingResult.getContentLines()).hasSize(4);
        assertThat(parsingResult.getContentLines().get(0)).isEqualTo("content line 1");
        assertThat(parsingResult.getContentLines().get(1)).isEqualTo("content line 2");
        assertThat(parsingResult.getContentLines().get(2)).isEqualTo("--- contentproperty: x");
        assertThat(parsingResult.getContentLines().get(3)).isEqualTo("# content comment");
        assertThat(parsingResult.getContentAsSingleLine()).isEqualTo("content line 1 content line 2 --- contentproperty: x # content comment");
    }

    @Test
    public void parseFileNoCommentProperties()
            throws IOException
    {
        String fileContent = "content line 1\n" +
                "content line 2";
        SectionParsingResult parsingResult = parseOnlySection(fileContent);

        assertThat(parsingResult.getProperty("unknownProperty")).isNotPresent();
        assertThat(parsingResult.getContentLines()).hasSize(2);
        assertThat(parsingResult.getContentLines().get(0)).isEqualTo("content line 1");
        assertThat(parsingResult.getContentLines().get(1)).isEqualTo("content line 2");
        assertThat(parsingResult.getContentAsSingleLine()).isEqualTo("content line 1 content line 2");
    }

    @Test
    public void shouldFailRedundantOptions()
    {
        String fileContent = "-- property1: value\n" +
                "-- property1: value2";

        assertThatThrownBy(() -> parseOnlySection(fileContent))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Different properties:  [{property1=(value, value2)}]");
    }

    @Test
    public void parseMultiSectionFile()
            throws IOException
    {
        String fileContent = "-- property1: value1; property2: value2; name: section1\n" +
                "content1\n" +
                "--! name: section2\n" +
                "-- property3: value3\n" +
                "content2";
        List<SectionParsingResult> sections = fileParser.parseFile(toInputStream(fileContent, UTF_8));

        assertThat(sections).hasSize(2);
        assertThat(sections.get(0).getProperty("property1")).hasValue("value1");
        assertThat(sections.get(0).getProperty("property2")).hasValue("value2");
        assertThat(sections.get(0).getSectionName()).hasValue("section1");
        assertThat(sections.get(0).getContentLines()).hasSize(1);
        assertThat(sections.get(0).getContentLines()).isEqualTo(List.of("content1"));
    }

    @Test
    public void handlesEmptySections()
            throws IOException
    {
        String fileContent = "--! name: section1\n" +
                "--! name: section2";
        List<SectionParsingResult> sections = fileParser.parseFile(toInputStream(fileContent, UTF_8));

        assertThat(sections).hasSize(2);
        assertThat(sections.get(0).getSectionName()).hasValue("section1");
        assertThat(sections.get(1).getSectionName()).hasValue("section2");
    }

    @Test
    public void handlesLineEndEscapes()
            throws IOException
    {
        String fileContent = "--! name: section1\n" +
                "line\\n1\n" +
                "line2\n" +
                "--! name: section2";
        List<SectionParsingResult> sections = fileParser.parseFile(toInputStream(fileContent, UTF_8));

        assertThat(sections).hasSize(2);
        assertThat(sections.get(0).getSectionName()).hasValue("section1");
        assertThat(sections.get(0).getContentLines()).isEqualTo(List.of("line\n1", "line2"));
        assertThat(sections.get(1).getSectionName()).hasValue("section2");
    }

    private SectionParsingResult parseOnlySection(String fileContent)
            throws IOException
    {
        return getOnlyElement(fileParser.parseFile(toInputStream(fileContent, UTF_8)));
    }
}
