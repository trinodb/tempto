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

import com.google.common.base.Joiner;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.internal.convention.SqlResultDescriptor;
import io.trino.tempto.internal.query.QueryRowMapper;
import io.trino.tempto.query.QueryExecutionException;
import io.trino.tempto.query.QueryExecutor;
import io.trino.tempto.query.QueryResult;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.AbstractThrowableAssert;
import org.assertj.core.api.AssertProvider;
import org.assertj.core.api.Assertions;
import org.intellij.lang.annotations.Language;
import org.slf4j.Logger;

import javax.annotation.CheckReturnValue;

import java.sql.JDBCType;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Lists.newArrayList;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.internal.configuration.TestConfigurationFactory.testConfiguration;
import static io.trino.tempto.query.QueryResult.fromSqlIndex;
import static io.trino.tempto.query.QueryResult.toSqlIndex;
import static java.lang.String.format;
import static java.sql.JDBCType.INTEGER;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;
import static org.slf4j.LoggerFactory.getLogger;

public class QueryAssert
        extends AbstractAssert<QueryAssert, QueryResult>
{
    private static final Logger LOGGER = getLogger(QueryExecutor.class);

    private static final NumberFormat DECIMAL_FORMAT = new DecimalFormat("#0.00000000000");

    private final List<ValueComparator> columnComparators;
    private final List<JDBCType> columnTypes;

    private QueryAssert(QueryResult actual)
    {
        super(actual, QueryAssert.class);
        this.columnComparators = getComparators(actual);
        this.columnTypes = actual.getColumnTypes();
    }

    /**
     * Use {@link Assertions#assertThat(AssertProvider)}.
     */
    @CheckReturnValue
    @Deprecated
    public static QueryAssert assertThat(QueryResult queryResult)
    {
        return new QueryAssert(queryResult);
    }

    /**
     * @deprecated Use {@link #assertQueryFailure(QueryCallback)} instead.
     */
    @Deprecated
    @CheckReturnValue
    public static QueryExecutionAssert assertThat(QueryCallback queryCallback)
    {
        QueryExecutionException executionException = null;
        try {
            queryCallback.executeQuery();
        }
        catch (QueryExecutionException e) {
            executionException = e;
        }
        return new QueryExecutionAssert(ofNullable(executionException));
    }

    @CheckReturnValue
    public static AbstractThrowableAssert<?, ? extends Throwable> assertQueryFailure(QueryCallback queryCallback)
    {
        QueryExecutionException executionException = null;
        try {
            queryCallback.executeQuery();
        }
        catch (QueryExecutionException e) {
            return Assertions.assertThat(e.getCause());
        }
        throw new AssertionError("Expected callback to throw QueryExecutionException");
    }

    public QueryAssert matches(SqlResultDescriptor sqlResultDescriptor)
    {
        if (sqlResultDescriptor.getExpectedTypes().isPresent()) {
            hasColumns(sqlResultDescriptor.getExpectedTypes().get());
        }

        List<Row> rows;
        try {
            rows = sqlResultDescriptor.getRows(columnTypes);
        }
        catch (Exception e) {
            throw new RuntimeException(
                    format("Could not map expected file content to query column types; types=%s; content=<%s>", columnTypes, sqlResultDescriptor.getOriginalContent()),
                    e);
        }

        if (sqlResultDescriptor.isIgnoreOrder()) {
            contains(rows);
        }
        else {
            containsExactly(rows);
        }

        if (!sqlResultDescriptor.isIgnoreExcessRows()) {
            hasRowsCount(rows.size());
        }

        return this;
    }

    public QueryAssert hasRowsCount(int resultCount)
    {
        if (actual.getRowsCount() != resultCount) {
            failWithMessage("Expected row count to be <%s>, but was <%s>; rows=%s", resultCount, actual.getRowsCount(), actual.rows());
        }
        return this;
    }

    public QueryAssert hasNoRows()
    {
        return hasRowsCount(0);
    }

    public QueryAssert hasAnyRows()
    {
        if (actual.getRowsCount() == 0) {
            failWithMessage("Expected some rows to be returned from query");
        }
        return this;
    }

    public QueryAssert hasColumnsCount(int columnCount)
    {
        if (actual.getColumnsCount() != columnCount) {
            failWithMessage("Expected column count to be <%s>, but was <%s> - columns <%s>", columnCount, actual.getColumnsCount(), actual.getColumnTypes());
        }
        return this;
    }

    public QueryAssert hasColumns(List<JDBCType> expectedTypes)
    {
        hasColumnsCount(expectedTypes.size());
        for (int i = 0; i < expectedTypes.size(); i++) {
            JDBCType expectedType = expectedTypes.get(i);
            JDBCType actualType = actual.getColumnType(toSqlIndex(i));

            if (!actualType.equals(expectedType)) {
                failWithMessage("Expected <%s> column of type <%s>, but was <%s>, actual columns: %s", i, expectedType, actualType, actual.getColumnTypes());
            }
        }
        return this;
    }

    public QueryAssert hasColumns(JDBCType... expectedTypes)
    {
        return hasColumns(Arrays.asList(expectedTypes));
    }

    /**
     * Verifies that the actual result set contains all the given {@code rows}
     *
     * @param rows Rows to be matched
     * @return this
     */
    public QueryAssert contains(List<Row> rows)
    {
        List<List<?>> missingRows = newArrayList();
        for (Row row : rows) {
            List<?> expectedRow = row.getValues();

            if (!containsRow(expectedRow)) {
                missingRows.add(expectedRow);
            }
        }

        if (!missingRows.isEmpty()) {
            failWithMessage("%s", buildContainsMessage(missingRows));
        }

        return this;
    }

    /**
     * @param rows Rows to be matched
     * @return this
     * @see #contains(java.util.List)
     */
    public QueryAssert contains(Row... rows)
    {
        return contains(Arrays.asList(rows));
    }

    /**
     * Verifies that the actual result set consist of only {@code rows} in any order
     *
     * @param rows Rows to be matched
     * @return this
     */
    public QueryAssert containsOnly(List<Row> rows)
    {
        hasRowsCount(rows.size());
        contains(rows);

        return this;
    }

    /**
     * @param rows Rows to be matched
     * @return this
     * @see #containsOnly(java.util.List)
     */
    public QueryAssert containsOnly(Row... rows)
    {
        return containsOnly(Arrays.asList(rows));
    }

    /**
     * @deprecated Use {@link #containsExactlyInOrder(List)}
     */
    @Deprecated
    public QueryAssert containsExactly(List<Row> rows)
    {
        return containsExactlyInOrder(rows);
    }

    /**
     * Verifies that the actual result set equals to {@code rows}.
     * ResultSet in different order or with any extra rows perceived as not same
     *
     * @param rows Rows to be matched
     * @return this
     */
    public QueryAssert containsExactlyInOrder(List<Row> rows)
    {
        hasRowsCount(rows.size());
        List<Integer> unequalRowsIndexes = newArrayList();
        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
            List<?> expectedRow = rows.get(rowIndex).getValues();
            List<?> actualRow = actual.row(rowIndex);

            if (!rowsEqual(expectedRow, actualRow)) {
                unequalRowsIndexes.add(rowIndex);
            }
        }

        if (!unequalRowsIndexes.isEmpty()) {
            failWithMessage("%s", buildContainsExactlyErrorMessage(unequalRowsIndexes, rows));
        }

        return this;
    }

    /**
     * @deprecated Use {@link #containsExactlyInOrder(Row...)}
     */
    @Deprecated
    public QueryAssert containsExactly(Row... rows)
    {
        return containsExactlyInOrder(rows);
    }

    /**
     * @param rows Rows to be matched
     * @return this
     * @see #containsExactlyInOrder(java.util.List)
     */
    public QueryAssert containsExactlyInOrder(Row... rows)
    {
        return containsExactly(Arrays.asList(rows));
    }

    /**
     * Verifies number of rows updated/inserted by last update query
     *
     * @param count Number of rows expected
     * @return this
     */
    public QueryAssert updatedRowsCountIsEqualTo(int count)
    {
        hasRowsCount(1);
        hasColumnsCount(1);
        hasColumns(INTEGER);
        containsExactly(row(count));
        return this;
    }

    private static List<ValueComparator> getComparators(QueryResult queryResult)
    {
        Configuration configuration = testConfiguration();
        return queryResult.getColumnTypes().stream()
                .map(it -> QueryResultValueComparator.comparatorForType(it, configuration))
                .collect(Collectors.toList());
    }

    private String buildContainsMessage(List<List<?>> missingRows)
    {
        StringBuilder msg = new StringBuilder("Could not find rows:");
        appendRows(msg, missingRows);
        msg.append("\n\nactual rows:");
        appendRows(msg, actual.rows());
        return msg.toString();
    }

    private void appendRows(StringBuilder msg, List<List<?>> rows)
    {
        rows.stream()
                .map(QueryAssert::rowToString)
                .forEach(row -> msg.append('\n').append(row));
    }

    private static String rowToString(List<?> row)
    {
        return row.stream()
                .map(Row::valueToString)
                .collect(joining(", ", "[", "]"));
    }

    private String buildContainsExactlyErrorMessage(List<Integer> unequalRowsIndexes, List<Row> rows)
    {
        StringBuilder msg = new StringBuilder("Not equal rows:");
        for (Integer unequalRowsIndex : unequalRowsIndexes) {
            int unequalRowIndex = unequalRowsIndex;
            msg.append('\n');
            msg.append(unequalRowIndex);
            msg.append(" - expected: ");
            msg.append(rows.get(unequalRowIndex));
            msg.append('\n');
            msg.append(unequalRowIndex);
            msg.append(" - actual:   ");
            msg.append(new Row(actual.row(unequalRowIndex)));
        }
        return msg.toString();
    }

    private boolean containsRow(List<?> expectedRow)
    {
        for (int i = 0; i < actual.getRowsCount(); i++) {
            if (rowsEqual(expectedRow, actual.row(i))) {
                return true;
            }
        }
        return false;
    }

    private boolean rowsEqual(List<?> expectedRow, List<?> actualRow)
    {
        if (expectedRow.size() != actualRow.size()) {
            return false;
        }
        for (int i = 0; i < expectedRow.size(); ++i) {
            List<?> acceptableValues = expectedRow.get(i) instanceof AcceptableValues ?
                    ((AcceptableValues) expectedRow.get(i)).getValues()
                    : singletonList(expectedRow.get(i));
            Object actualValue = actualRow.get(i);

            if (!isAnyValueEqual(i, acceptableValues, actualValue)) {
                return false;
            }
        }
        return true;
    }

    private boolean isAnyValueEqual(int column, List<?> expectedValues, Object actualValue)
    {
        for (Object expectedValue : expectedValues) {
            if (columnComparators.get(column).test(actualValue, expectedValue)) {
                return true;
            }
        }
        return false;
    }

    public <T> QueryAssert column(int columnIndex, JDBCType type, ColumnValuesAssert<T> columnValuesAssert)
    {
        if (fromSqlIndex(columnIndex) > actual.getColumnsCount()) {
            failWithMessage("Result contains only <%s> columns, extracting column <%s>",
                    actual.getColumnsCount(), columnIndex);
        }

        JDBCType actualColumnType = actual.getColumnType(columnIndex);
        if (!type.equals(actualColumnType)) {
            failWithMessage("Expected <%s> column, to be type: <%s>, but was: <%s>", columnIndex, type, actualColumnType);
        }

        List<T> columnValues = actual.column(columnIndex);

        columnValuesAssert.assertColumnValues(Assertions.assertThat(columnValues));

        return this;
    }

    public <T> QueryAssert column(String columnName, JDBCType type, ColumnValuesAssert<T> columnValuesAssert)
    {
        Optional<Integer> index = actual.tryFindColumnIndex(columnName);
        if (!index.isPresent()) {
            failWithMessage("No column with name: <%s>", columnName);
        }

        return column(index.get(), type, columnValuesAssert);
    }

    public static AcceptableValues anyOf(Object... values)
    {
        return new AcceptableValues(Arrays.asList(values));
    }

    @FunctionalInterface
    public interface QueryCallback
    {
        QueryResult executeQuery()
                throws QueryExecutionException;
    }

    @Deprecated
    public static class QueryExecutionAssert
    {
        private Optional<QueryExecutionException> executionExceptionOptional;

        public QueryExecutionAssert(Optional<QueryExecutionException> executionExceptionOptional)
        {
            this.executionExceptionOptional = executionExceptionOptional;
        }

        private QueryExecutionException getRequiredFailure()
        {
            return executionExceptionOptional
                    .orElseThrow(() -> new AssertionError("Query did not fail as expected."));
        }

        private String getFailureMessage()
        {
            return nullToEmpty(getRequiredFailure().getMessage());
        }

        public QueryExecutionAssert failsWithMessage(String expectedErrorMessage)
        {
            String exceptionMessage = getFailureMessage();
            LOGGER.debug("Query failed as expected, with message: {}", exceptionMessage);
            if (!exceptionMessage.contains(expectedErrorMessage)) {
                AssertionError error = new AssertionError(format(
                        "Query failed with unexpected error message: '%s' \n Expected error message to contain '%s'",
                        exceptionMessage,
                        expectedErrorMessage));
                error.addSuppressed(getRequiredFailure());
                throw error;
            }

            return this;
        }

        public QueryExecutionAssert failsWithMessageMatching(@Language("RegExp") String expectedErrorMessagePattern)
        {
            requireNonNull(expectedErrorMessagePattern, "expectedErrorMessagePattern is null");
            String exceptionMessage = getFailureMessage();
            LOGGER.debug("Query failed as expected, with message: {}", exceptionMessage);
            if (!exceptionMessage.matches(expectedErrorMessagePattern)) {
                AssertionError error = new AssertionError(format(
                        "Query failed with unexpected error message: '%s' \n Expected error message to match '%s'",
                        exceptionMessage,
                        expectedErrorMessagePattern));
                error.addSuppressed(getRequiredFailure());
                throw error;
            }

            return this;
        }
    }

    public static class Row
    {
        private final List<?> values;

        public Row(Object... values)
        {
            this(newArrayList(values));
        }

        public Row(List<?> values)
        {
            this.values = unmodifiableList(new ArrayList<>(requireNonNull(values, "values is null")));
        }

        public List<?> getValues()
        {
            return values;
        }

        public static Row row(Object... values)
        {
            return new Row(values);
        }

        @Override
        public String toString()
        {
            return values.stream()
                    .map(Row::valueToString)
                    .collect(joining("|", "", "|"));
        }

        static String valueToString(Object value)
        {
            if (value == null) {
                return "null";
            }

            if (value instanceof Double || value instanceof Float) {
                return DECIMAL_FORMAT.format(value);
            }

            if (value.getClass().isArray()) {
                String wrapped = Arrays.deepToString(new Object[] {value});
                verify(wrapped.charAt(0) == '[' && wrapped.charAt(wrapped.length() - 1) == ']'); // guaranteed by Arrays.deepToString
                return wrapped.substring(1, wrapped.length() - 1);
            }

            return value.toString();
        }
    }

    public static class AcceptableValues
    {
        private final List<?> values;

        public AcceptableValues(List<?> values)
        {
            this.values = unmodifiableList(new ArrayList<>(requireNonNull(values, "values can not be null")));
        }

        public List<?> getValues()
        {
            return values;
        }

        @Override
        public String toString()
        {
            String jointValues = Joiner.on(", ")
                    .useForNull(QueryRowMapper.NULL_STRING)
                    .join(values);
            return "anyOf(" + jointValues + ")";
        }
    }
}
