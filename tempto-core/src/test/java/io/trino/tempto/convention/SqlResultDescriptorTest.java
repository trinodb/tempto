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

package io.trino.tempto.convention;

import io.trino.tempto.assertions.QueryAssert;
import io.trino.tempto.internal.convention.AnnotatedFileParser;
import io.trino.tempto.internal.convention.AnnotatedFileParser.SectionParsingResult;
import io.trino.tempto.internal.convention.SqlResultDescriptor;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.sql.JDBCType.BINARY;
import static java.sql.JDBCType.BIT;
import static java.sql.JDBCType.DATE;
import static java.sql.JDBCType.INTEGER;
import static java.sql.JDBCType.NUMERIC;
import static java.sql.JDBCType.REAL;
import static java.sql.JDBCType.TIME;
import static java.sql.JDBCType.TIMESTAMP;
import static java.sql.JDBCType.VARCHAR;
import static org.apache.commons.io.IOUtils.toInputStream;
import static org.assertj.core.api.Assertions.assertThat;

public class SqlResultDescriptorTest
{
    @Test
    public void sampleResultFile()
            throws IOException
    {
        String fileContent = """
                -- delimiter: |; ignoreOrder: true; types: VARCHAR|BINARY|BIT|INTEGER|REAL|NUMERIC|DATE|TIME|TIMESTAMP
                A|abcd|1|10|20.0|30.0|2015-11-01|10:55:25|2016-11-01 10:55:25|
                B|abcd|1|10|20.0|30.0|2015-11-01|10:55:25|2016-11-01 10:55:25|""";
        SqlResultDescriptor resultDescriptor = parse(fileContent);

        assertThat(resultDescriptor.isIgnoreOrder()).isTrue();
        assertThat(resultDescriptor.isJoinAllRowsToOne()).isFalse();
        List<JDBCType> expectedTypes = List.of(VARCHAR, BINARY, BIT, INTEGER, REAL, NUMERIC, DATE, TIME, TIMESTAMP);
        assertThat(resultDescriptor.getExpectedTypes()).isEqualTo(Optional.of(expectedTypes));
        List<QueryAssert.Row> rows = resultDescriptor.getRows(expectedTypes);
        assertThat(rows).hasSize(2);

        List<?> values = rows.get(0).getValues();
        assertThat(values.get(0)).isEqualTo("A");
        assertThat((byte[]) values.get(1)).isEqualTo(new byte[] {(byte) 0xab, (byte) 0xcd});
        assertThat(values.get(2)).isEqualTo(true);
        assertThat(values.get(3)).isEqualTo(10);
        assertThat(values.get(4)).isEqualTo(Double.valueOf(20.0));
        assertThat(values.get(5)).isEqualTo(new BigDecimal("30.0"));
        assertThat(values.get(6)).isEqualTo(Date.valueOf("2015-11-01"));
        assertThat(values.get(7)).isEqualTo(Time.valueOf("10:55:25"));
        assertThat(values.get(8)).isEqualTo(Timestamp.valueOf("2016-11-01 10:55:25"));
    }

    @Test
    public void sampleResultFileWithoutExplicitExpectedTypes()
            throws IOException
    {
        String fileContent = """
                -- delimiter: |; ignoreOrder: false
                A|
                B|""";
        SqlResultDescriptor resultDescriptor = parse(fileContent);

        assertThat(resultDescriptor.isIgnoreOrder()).isFalse();
        assertThat(resultDescriptor.isJoinAllRowsToOne()).isFalse();
        assertThat(resultDescriptor.getExpectedTypes()).isNotPresent();
        List<QueryAssert.Row> rows = resultDescriptor.getRows(List.of(VARCHAR));
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).getValues()).isEqualTo(List.of("A"));
        assertThat(rows.get(1).getValues()).isEqualTo(List.of("B"));
    }

    @Test
    public void joinAllRowsToOneFile()
            throws IOException
    {
        String fileContent = """
                -- delimiter: |; ignoreOrder: true; joinAllRowsToOne: true; types: VARCHAR
                A|
                B|""";
        SqlResultDescriptor resultDescriptor = parse(fileContent);

        assertThat(resultDescriptor.isIgnoreOrder()).isTrue();
        assertThat(resultDescriptor.isJoinAllRowsToOne()).isTrue();

        List<JDBCType> expectedTypes = List.of(VARCHAR);
        assertThat(resultDescriptor.getExpectedTypes()).isEqualTo(Optional.of(expectedTypes));
        List<QueryAssert.Row> rows = resultDescriptor.getRows(expectedTypes);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getValues()).isEqualTo(List.of("A\nB"));
    }

    @Test
    public void recordWithEmptyLines()
            throws IOException
    {
        String fileContent = "-- delimiter: |; joinAllRowsToOne: true; types: VARCHAR\n" +
                "This\nrecord\n\\\nhas\n\\\n\\\nempty\n\\\n\\\n\\\nlines\n\\\n|";
        SqlResultDescriptor resultDescriptor = parse(fileContent);

        assertThat(resultDescriptor.isJoinAllRowsToOne()).isTrue();

        List<JDBCType> expectedTypes = List.of(VARCHAR);
        assertThat(resultDescriptor.getExpectedTypes()).isEqualTo(Optional.of(expectedTypes));
        List<QueryAssert.Row> rows = resultDescriptor.getRows(expectedTypes);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getValues()).isEqualTo(List.of("This\nrecord\n\nhas\n\n\nempty\n\n\n\nlines\n\n"));
    }

    private SqlResultDescriptor parse(String fileContent)
            throws IOException
    {
        SectionParsingResult parsingResult = getOnlyElement(new AnnotatedFileParser().parseFile(toInputStream(fileContent, UTF_8)));
        return new SqlResultDescriptor(parsingResult);
    }
}
