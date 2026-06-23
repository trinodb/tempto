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

package io.trino.tempto.assertions;

import io.trino.tempto.internal.convention.AnnotatedFileParser;
import io.trino.tempto.internal.convention.AnnotatedFileParser.SectionParsingResult;
import io.trino.tempto.internal.convention.SqlResultDescriptor;
import io.trino.tempto.query.QueryExecutionException;
import io.trino.tempto.query.QueryResult;
import org.assertj.core.api.AbstractListAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ObjectAssert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.ResultSet;
import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.anyOf;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.internal.configuration.TestConfigurationFactory.TEST_CONFIGURATION_URIS_KEY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.sql.JDBCType.BIGINT;
import static java.sql.JDBCType.INTEGER;
import static java.sql.JDBCType.VARBINARY;
import static java.sql.JDBCType.VARCHAR;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;

public class QueryAssertTest
{
    private final QueryResult NATION_JOIN_REGION_QUERY_RESULT = buildNationJoinRegionQueryResult();

    private final QueryResult QUERY_RESULT_WITH_VARBINARY = buildQueryResultWithVarbinary();

    private final ColumnValuesAssert<Object> EMPTY_COLUMN_VALUE_ASSERT = new ColumnValuesAssert<Object>()
    {
        @Override
        public void assertColumnValues(AbstractListAssert<?, ? extends List<?>, Object, ObjectAssert<Object>> columnAssert)
        {
            // DO NOTHING
        }
    };

    @BeforeAll
    public static void setupSpec()
    {
        System.setProperty(TEST_CONFIGURATION_URIS_KEY, "/configuration/global-configuration-tempto.yaml");
    }

    private static QueryResult buildNationJoinRegionQueryResult()
    {
        QueryResult.QueryResultBuilder builder = new QueryResult.QueryResultBuilder(
                asList(BIGINT, VARCHAR, VARCHAR),
                asList("n.nationkey", "n.name", "r.name"));
        builder.addRow(1, "ALGERIA", "AFRICA");
        builder.addRow(2, "ARGENTINA", "SOUTH AMERICA");
        builder.setJdbcResultSet(mock(ResultSet.class));
        return builder.build();
    }

    private static QueryResult buildQueryResultWithVarbinary()
    {
        QueryResult.QueryResultBuilder builder = new QueryResult.QueryResultBuilder(
                asList(VARCHAR, VARBINARY),
                asList("key", "value"));
        builder.addRow("one", "one".getBytes(UTF_8));
        builder.addRow("two", "two".getBytes(UTF_8));
        builder.setJdbcResultSet(mock(ResultSet.class));
        return builder.build();
    }

    @Test
    public void hasResultCountFails()
    {
        assertThatThrownBy(() -> assertThat(NATION_JOIN_REGION_QUERY_RESULT).hasRowsCount(3))
                .isInstanceOf(AssertionError.class)
                .hasMessageStartingWith("Expected row count to be <3>, but was <2>; rows=");
    }

    @Test
    public void hasResultCount()
    {
        assertThat(NATION_JOIN_REGION_QUERY_RESULT).hasRowsCount(2);
    }

    @Test
    public void hasAnyRowsCorrect()
    {
        assertThat(NATION_JOIN_REGION_QUERY_RESULT).hasAnyRows();
    }

    @Test
    public void hasNoRowsFails()
    {
        assertThatThrownBy(() -> assertThat(NATION_JOIN_REGION_QUERY_RESULT).hasNoRows())
                .isInstanceOf(AssertionError.class)
                .hasMessageStartingWith("Expected row count to be <0>, but was <2>; rows=");
    }

    @Test
    public void extractingColumnFailsNoSuchColumnIndex()
    {
        assertThatThrownBy(() -> assertThat(NATION_JOIN_REGION_QUERY_RESULT)
                .column(30, INTEGER, EMPTY_COLUMN_VALUE_ASSERT))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Result contains only <3> columns, extracting column <30>");
    }

    @Test
    public void extractingColumnFailsNoSuchColumnName()
    {
        assertThatThrownBy(() -> assertThat(NATION_JOIN_REGION_QUERY_RESULT)
                .column("unknown_column", INTEGER, EMPTY_COLUMN_VALUE_ASSERT))
                .isInstanceOf(AssertionError.class)
                .hasMessage("No column with name: <unknown_column>");
    }

    @Test
    public void extractingColumnFailsInvalidType()
    {
        assertThatThrownBy(() -> assertThat(NATION_JOIN_REGION_QUERY_RESULT)
                .column("n.nationkey", VARCHAR, EMPTY_COLUMN_VALUE_ASSERT))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Expected <1> column, to be type: <VARCHAR>, but was: <BIGINT>");
    }

    @Test
    public void hasColumnCountWithIndex()
    {
        assertThat(NATION_JOIN_REGION_QUERY_RESULT)
                .column(1, BIGINT, EMPTY_COLUMN_VALUE_ASSERT);
    }

    @Test
    public void hasColumnCountWithName()
    {
        assertThat(NATION_JOIN_REGION_QUERY_RESULT)
                .column("n.nationkey", BIGINT, EMPTY_COLUMN_VALUE_ASSERT);
    }

    @Test
    public void hasColumnsWrongColumnCount()
    {
        assertThatThrownBy(() -> assertThat(NATION_JOIN_REGION_QUERY_RESULT).hasColumns(VARCHAR))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Expected column count to be <1>, but was <3> - columns <[BIGINT, VARCHAR, VARCHAR]>");
    }

    @Test
    public void hasColumnsDifferentColumnTypes()
    {
        assertThatThrownBy(() -> assertThat(NATION_JOIN_REGION_QUERY_RESULT).hasColumns(VARCHAR, VARCHAR, VARCHAR))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Expected <0> column of type <VARCHAR>, but was <BIGINT>, actual columns: [BIGINT, VARCHAR, VARCHAR]");
    }

    @Test
    public void hasColumns()
    {
        assertThat(NATION_JOIN_REGION_QUERY_RESULT).hasColumns(BIGINT, VARCHAR, VARCHAR);
    }

    @Test
    public void hasRowsDifferentNumberOfRows()
    {
        assertThat(NATION_JOIN_REGION_QUERY_RESULT)
                .contains(
                        row(1, "ALGERIA", "AFRICA"),
                        row(2, "ARGENTINA", "SOUTH AMERICA"));
    }

    @Test
    public void hasRowsDifferentValueNoRow()
    {
        assertThatThrownBy(() -> assertThat(NATION_JOIN_REGION_QUERY_RESULT)
                .contains(
                        row(2, "ARGENTINA", "SOUTH AMERICA"),
                        row(1, "ALGERIA", "valid_value")))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Could not find rows:\n" +
                        "[1, ALGERIA, valid_value]\n" +
                        "\n" +
                        "actual rows:\n" +
                        "[1, ALGERIA, AFRICA]\n" +
                        "[2, ARGENTINA, SOUTH AMERICA]");
    }

    @Test
    public void hasRows()
    {
        assertThat(NATION_JOIN_REGION_QUERY_RESULT)
                .contains(
                        row(2, "ARGENTINA", "SOUTH AMERICA"),
                        row(1, "ALGERIA", "AFRICA"));
    }

    @Test
    public void hasRowsMissingSuffixColumn()
    {
        assertThatThrownBy(() -> assertThat(NATION_JOIN_REGION_QUERY_RESULT)
                .contains(
                        row(2, "ARGENTINA"),
                        row(1, "ALGERIA")))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Could not find rows:\n" +
                        "[2, ARGENTINA]\n" +
                        "[1, ALGERIA]\n" +
                        "\n" +
                        "actual rows:\n" +
                        "[1, ALGERIA, AFRICA]\n" +
                        "[2, ARGENTINA, SOUTH AMERICA]");
    }

    @Test
    public void hasRowsMissingMiddleColumn()
    {
        assertThatThrownBy(() -> assertThat(NATION_JOIN_REGION_QUERY_RESULT)
                .contains(
                        row(2, "SOUTH AMERICA"),
                        row(1, "AFRICA")))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Could not find rows:\n" +
                        "[2, SOUTH AMERICA]\n" +
                        "[1, AFRICA]\n" +
                        "\n" +
                        "actual rows:\n" +
                        "[1, ALGERIA, AFRICA]\n" +
                        "[2, ARGENTINA, SOUTH AMERICA]");
    }

    @Test
    public void hasRowsWithMultiplePossibleValues()
    {
        assertThat(NATION_JOIN_REGION_QUERY_RESULT)
                .contains(
                        row(2, "ARGENTINA", "SOUTH AMERICA"),
                        row(1, "ALGERIA", anyOf("AFRICA", "MARS")));
    }

    @Test
    public void hasRowsWithMultiplePossibleValuesNoRowMatching()
    {
        assertThatThrownBy(() -> assertThat(NATION_JOIN_REGION_QUERY_RESULT)
                .contains(
                        row(2, "ARGENTINA", "SOUTH AMERICA"),
                        row(1, "ALGERIA", anyOf("SATURN", null))))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Could not find rows:\n" +
                        "[1, ALGERIA, anyOf(SATURN, null)]\n" +
                        "\n" +
                        "actual rows:\n" +
                        "[1, ALGERIA, AFRICA]\n" +
                        "[2, ARGENTINA, SOUTH AMERICA]");
    }

    @Test
    public void hasRowsWithPercentQuery()
    {
        QueryResult.QueryResultBuilder builder = new QueryResult.QueryResultBuilder(
                asList(VARCHAR),
                asList("value"));
        builder.addRow("%,");
        builder.setJdbcResultSet(mock(ResultSet.class));
        QueryResult queryResult = builder.build();

        assertThatThrownBy(() -> assertThat(queryResult).contains(row("value %,")))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Could not find rows:\n" +
                        "[value %,]\n" +
                        "\n" +
                        "actual rows:\n" +
                        "[%,]");
    }

    @Test
    public void hasRowsInOrderDifferentNumberOfRows()
    {
        assertThatThrownBy(() -> assertThat(NATION_JOIN_REGION_QUERY_RESULT)
                .containsExactly(
                        row(1, "ALGERIA", "AFRICA"),
                        row(2, "ARGENTINA", "SOUTH AMERICA"),
                        row(3, "AUSTRIA", "EUROPE")))
                .isInstanceOf(AssertionError.class)
                .hasMessageStartingWith("Expected row count to be <3>, but was <2>; rows=");
    }

    @Test
    public void hasRowsInOrderDifferentValueNoRow()
    {
        assertThatThrownBy(() -> assertThat(NATION_JOIN_REGION_QUERY_RESULT)
                .containsExactly(
                        row(1, "ALGERIA", "AFRICA"),
                        row(2, "ARGENTINA", "valid_value")))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Not equal rows:\n" +
                        "1 - expected: 2|ARGENTINA|valid_value|\n1 - actual:   2|ARGENTINA|SOUTH AMERICA|");
    }

    @Test
    public void hasRowsInOrderDifferentOrder()
    {
        assertThatThrownBy(() -> assertThat(NATION_JOIN_REGION_QUERY_RESULT)
                .containsExactly(
                        row(2, "ARGENTINA", "SOUTH AMERICA"),
                        row(1, "ALGERIA", "AFRICA")))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Not equal rows:\n" +
                        "0 - expected: 2|ARGENTINA|SOUTH AMERICA|\n0 - actual:   1|ALGERIA|AFRICA|\n" +
                        "1 - expected: 1|ALGERIA|AFRICA|\n1 - actual:   2|ARGENTINA|SOUTH AMERICA|");
    }

    @Test
    public void hasRowsInOrder()
    {
        assertThat(NATION_JOIN_REGION_QUERY_RESULT)
                .containsExactly(
                        row(1, "ALGERIA", "AFRICA"),
                        row(2, "ARGENTINA", "SOUTH AMERICA"));
    }

    @Test
    public void matchesFileOkWithTypes()
    {
        SectionParsingResult parsingResult = parseResultFor("""
                -- delimiter: |; ignoreOrder: false; types: BIGINT|VARCHAR|VARCHAR
                1|ALGERIA|AFRICA|
                2|ARGENTINA|SOUTH AMERICA|
                """);

        assertThat(NATION_JOIN_REGION_QUERY_RESULT).matches(new SqlResultDescriptor(parsingResult));
    }

    @Test
    public void matchesFileFailedWrongExplicitTypesInResultFile()
    {
        SectionParsingResult parsingResult = parseResultFor("""
                -- delimiter: |; ignoreOrder: false; types: BIGINT|BIGINT|BIGINT
                1|ALGERIA|AFRICA|
                2|ARGENTINA|SOUTH AMERICA|
                """);

        assertThatThrownBy(() -> assertThat(NATION_JOIN_REGION_QUERY_RESULT).matches(new SqlResultDescriptor(parsingResult)))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Expected <1> column of type <BIGINT>, but was <VARCHAR>, actual columns: [BIGINT, VARCHAR, VARCHAR]");
    }

    @Test
    public void matchesFileOkNoExplicitTypes()
    {
        SectionParsingResult parsingResult = parseResultFor("""
                -- delimiter: |; ignoreOrder: false
                1|ALGERIA|AFRICA|
                2|ARGENTINA|SOUTH AMERICA|
                """);

        assertThat(NATION_JOIN_REGION_QUERY_RESULT).matches(new SqlResultDescriptor(parsingResult));
    }

    @Test
    public void matchesFileFailedWrongValue()
    {
        SectionParsingResult parsingResult = parseResultFor("""
                -- delimiter: |; ignoreOrder: false
                1|ALGERIA|AFRICA|
                3|ARGENTINA|SOUTH AMERICA|
                """);

        assertThatThrownBy(() -> assertThat(NATION_JOIN_REGION_QUERY_RESULT).matches(new SqlResultDescriptor(parsingResult)))
                .isInstanceOf(AssertionError.class)
                .hasMessage("""
                        Not equal rows:
                        1 - expected: 3|ARGENTINA|SOUTH AMERICA|
                        1 - actual:   2|ARGENTINA|SOUTH AMERICA|""");
    }

    @Test
    public void matchesFileFailedCannotMapExpectedResultToTypesFromDbResult()
    {
        SectionParsingResult parsingResult = parseResultFor("""
                -- delimiter: |; ignoreOrder: false
                A|ALGERIA|AFRICA|
                B|ARGENTINA|SOUTH AMERICA|
                """);

        assertThatThrownBy(() -> assertThat(NATION_JOIN_REGION_QUERY_RESULT).matches(new SqlResultDescriptor(parsingResult)))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("""
                        Could not map expected file content to query column types; types=[BIGINT, VARCHAR, VARCHAR]; content=<-- delimiter: |; ignoreOrder: false
                        A|ALGERIA|AFRICA|
                        B|ARGENTINA|SOUTH AMERICA|>""")
                .cause()
                .hasToString("java.lang.NumberFormatException: For input string: \"A\"");
    }

    private SectionParsingResult parseResultFor(String fileContent)
    {
        try {
            return getOnlyElement(new AnnotatedFileParser().parseFile(new ByteArrayInputStream(fileContent.getBytes())));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    public void queryExecutionAssertNotFailAsExpected()
    {
        assertThatThrownBy(() -> assertThat(() -> null).failsWithMessage("dummy"))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Query did not fail as expected.");
    }

    @Test
    public void queryExecutionAssertWrongErrorMessage()
    {
        AssertionError e = (AssertionError) catchThrowable(
                () -> assertThat(() -> {
                    throw new QueryExecutionException(new RuntimeException("foo bar"));
                }).failsWithMessage("dummy"));

        Assertions.assertThat(e).isInstanceOf(AssertionError.class);
        Assertions.assertThat(e.getMessage()).isEqualTo("Query failed with unexpected error message: 'java.lang.RuntimeException: foo bar' \n" +
                " Expected error message to contain 'dummy'");
        Throwable suppressed = e.getSuppressed()[0];
        Assertions.assertThat(suppressed.getClass()).isEqualTo(QueryExecutionException.class);
        Assertions.assertThat(suppressed.getMessage()).isEqualTo("java.lang.RuntimeException: foo bar");
    }

    @Test
    public void queryExecutionAssertRightErrorMessage()
    {
        assertThat(() -> {
            throw new QueryExecutionException(new RuntimeException("dummy"));
        }).failsWithMessage("dummy");
    }

    @Test
    public void queryExecutionAssertErrorMessageDoesNotMatch()
    {
        AssertionError e = (AssertionError) catchThrowable(
                () -> assertThat(() -> {
                    throw new QueryExecutionException(new RuntimeException("foo bar"));
                }).failsWithMessageMatching("foo"));

        Assertions.assertThat(e).isInstanceOf(AssertionError.class);
        Assertions.assertThat(e.getMessage()).isEqualTo("Query failed with unexpected error message: 'java.lang.RuntimeException: foo bar' \n" +
                " Expected error message to match 'foo'");
        Throwable suppressed = e.getSuppressed()[0];
        Assertions.assertThat(suppressed.getClass()).isEqualTo(QueryExecutionException.class);
        Assertions.assertThat(suppressed.getMessage()).isEqualTo("java.lang.RuntimeException: foo bar");
    }

    @Test
    public void queryExecutionAssertErrorMessageMatches()
    {
        assertThat(() -> {
            throw new QueryExecutionException(new RuntimeException("dummy"));
        }).failsWithMessageMatching("^java.lang.RuntimeException: dug?(m){2,4}y$");
    }

    @Test
    public void queryAssertWithVarbinary()
    {
        assertThatThrownBy(() -> assertThat(QUERY_RESULT_WITH_VARBINARY)
                .contains(row("three", "three".getBytes(UTF_8))))
                .isInstanceOf(AssertionError.class)
                .hasMessage("Could not find rows:\n" +
                        "[three, [116, 104, 114, 101, 101]]\n" +
                        "\n" +
                        "actual rows:\n" +
                        "[one, [111, 110, 101]]\n" +
                        "[two, [116, 119, 111]]");
    }
}
