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

import com.google.common.collect.ImmutableMap;
import io.trino.tempto.configuration.Configuration;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.stream.Stream;

import static java.sql.JDBCType.BIGINT;
import static java.sql.JDBCType.BINARY;
import static java.sql.JDBCType.BIT;
import static java.sql.JDBCType.CHAR;
import static java.sql.JDBCType.DATE;
import static java.sql.JDBCType.DECIMAL;
import static java.sql.JDBCType.DOUBLE;
import static java.sql.JDBCType.FLOAT;
import static java.sql.JDBCType.INTEGER;
import static java.sql.JDBCType.JAVA_OBJECT;
import static java.sql.JDBCType.LONGVARBINARY;
import static java.sql.JDBCType.LONGVARCHAR;
import static java.sql.JDBCType.NUMERIC;
import static java.sql.JDBCType.NVARCHAR;
import static java.sql.JDBCType.SMALLINT;
import static java.sql.JDBCType.STRUCT;
import static java.sql.JDBCType.TIME;
import static java.sql.JDBCType.TIMESTAMP;
import static java.sql.JDBCType.TIMESTAMP_WITH_TIMEZONE;
import static java.sql.JDBCType.TIME_WITH_TIMEZONE;
import static java.sql.JDBCType.TINYINT;
import static java.sql.JDBCType.VARBINARY;
import static java.sql.JDBCType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryResultValueComparatorTest
{
    @ParameterizedTest
    @MethodSource("comparatorData")
    public void queryResultValueComparator(JDBCType type, Object actual, Object expected, boolean result)
    {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getDouble(any())).thenReturn(Optional.empty());

        assertThat(QueryResultValueComparator.comparatorForType(type, configuration).test(actual, expected)).isEqualTo(result);
    }

    static Stream<Arguments> comparatorData()
    {
        return Stream.of(
                arguments(CHAR, null, null, true),
                arguments(CHAR, null, "a", false),
                arguments(CHAR, "a", null, false),

                arguments(CHAR, "a", "a", true),
                arguments(VARCHAR, "a", "a", true),
                arguments(NVARCHAR, "a", "a", true),
                arguments(LONGVARCHAR, "a", "a", true),
                arguments(VARCHAR, "a", "b", false),
                arguments(VARCHAR, "b", "a", false),

                arguments(BINARY, byteArray(0), byteArray(0), true),
                arguments(VARBINARY, byteArray(0), byteArray(0), true),
                arguments(LONGVARBINARY, byteArray(0), byteArray(0), true),
                arguments(BINARY, byteArray(0), byteArray(1), false),
                arguments(BINARY, byteArray(1), byteArray(0), false),

                arguments(BIT, true, true, true),
                arguments(BIT, true, false, false),
                arguments(BIT, false, true, false),

                arguments(BIGINT, 1L, 1, true),
                arguments(INTEGER, 1, 1, true),
                arguments(SMALLINT, (short) 1, 1, true),
                arguments(TINYINT, (byte) 1, 1, true),
                arguments(BIGINT, 1L, 0L, false),
                arguments(BIGINT, 0L, 1L, false),

                arguments(DOUBLE, Double.valueOf(0.0), Double.valueOf(0.0), true),
                arguments(DOUBLE, Double.valueOf(1.0), Double.valueOf(0.0), false),
                arguments(DOUBLE, Double.valueOf(0.0), Double.valueOf(1.0), false),

                arguments(DOUBLE, Double.valueOf(1.0), Double.valueOf(1.00000001), false),
                arguments(DOUBLE, Double.valueOf(1.0), Double.valueOf(1.000000000000001), false),
                arguments(DOUBLE, Double.valueOf(1.0), Double.valueOf(1.0000000000000001), true),
                arguments(FLOAT, Float.valueOf(1.0f), Float.valueOf(1.0000001f), false),
                arguments(FLOAT, Float.valueOf(1.0f), Float.valueOf(1.00000001f), true),

                arguments(DOUBLE, Double.NaN, Double.NaN, true),
                arguments(DOUBLE, Double.valueOf(1.0), Double.NaN, false),
                arguments(DOUBLE, Double.NaN, Double.valueOf(1.0), false),

                arguments(FLOAT, Float.NaN, Float.NaN, true),
                arguments(FLOAT, Float.valueOf(1.0f), Float.NaN, false),
                arguments(FLOAT, Float.NaN, Float.valueOf(1.0f), false),

                arguments(NUMERIC, BigDecimal.valueOf(0.0), BigDecimal.valueOf(0.0), true),
                arguments(DECIMAL, BigDecimal.valueOf(0.0), BigDecimal.valueOf(0.0), true),
                arguments(NUMERIC, BigDecimal.valueOf(1.0), BigDecimal.valueOf(0.0), false),
                arguments(NUMERIC, BigDecimal.valueOf(0.0), BigDecimal.valueOf(1.0), false),

                arguments(DATE, Date.valueOf("2015-02-15"), Date.valueOf("2015-02-15"), true),
                arguments(DATE, Date.valueOf("2015-02-16"), Date.valueOf("2015-02-15"), false),
                arguments(DATE, Date.valueOf("2015-02-15"), Date.valueOf("2015-02-16"), false),

                arguments(TIME, Time.valueOf("10:10:10"), Time.valueOf("10:10:10"), true),
                arguments(TIME_WITH_TIMEZONE, Time.valueOf("10:10:10"), Time.valueOf("10:10:10"), true),
                arguments(TIME, Time.valueOf("11:10:10"), Time.valueOf("10:10:10"), false),
                arguments(TIME, Time.valueOf("10:10:10"), Time.valueOf("11:10:10"), false),

                arguments(TIMESTAMP, Timestamp.valueOf("2015-02-15 10:10:10"), Timestamp.valueOf("2015-02-15 10:10:10"), true),
                arguments(TIMESTAMP_WITH_TIMEZONE, Timestamp.valueOf("2015-02-15 10:10:10"), Timestamp.valueOf("2015-02-15 10:10:10"), true),
                arguments(TIMESTAMP_WITH_TIMEZONE, ZonedDateTime.parse("2015-02-15T10:10:10Z"), ZonedDateTime.parse("2015-02-15T10:10:10Z"), true),
                arguments(TIMESTAMP_WITH_TIMEZONE, ZonedDateTime.parse("2015-02-15T10:10:10+01:00"), ZonedDateTime.parse("2015-02-15T10:10:10+01:00"), true),
                // same point in time, different zone
                arguments(TIMESTAMP_WITH_TIMEZONE, ZonedDateTime.parse("2015-02-15T11:10:10+02:00"), ZonedDateTime.parse("2015-02-15T10:10:10+01:00"), false),
                // same local time, different zone
                arguments(TIMESTAMP_WITH_TIMEZONE, ZonedDateTime.parse("2015-02-15T10:10:10+02:00"), ZonedDateTime.parse("2015-02-15T10:10:10+01:00"), false),
                arguments(TIMESTAMP, Timestamp.valueOf("2015-02-16 10:10:10"), Timestamp.valueOf("2015-02-15 10:10:10"), false),
                arguments(TIMESTAMP, Timestamp.valueOf("2015-02-15 10:10:10"), Timestamp.valueOf("2015-02-16 10:10:10"), false),

                arguments(STRUCT, null, null, true),
                arguments(STRUCT, null, ImmutableMap.of(), false),
                arguments(STRUCT, ImmutableMap.of(), null, false),
                arguments(STRUCT, ImmutableMap.of(), ImmutableMap.of(), true),
                arguments(STRUCT, ImmutableMap.of("a", "B"), ImmutableMap.of(), false),
                arguments(STRUCT, ImmutableMap.of("a", "B"), ImmutableMap.of("a", "b"), false),
                arguments(STRUCT, ImmutableMap.of("a", "B"), ImmutableMap.of("x", "y"), false),
                arguments(STRUCT, ImmutableMap.of(), ImmutableMap.of("x", "y"), false),

                arguments(JAVA_OBJECT, null, null, true),
                arguments(JAVA_OBJECT, null, ImmutableMap.of(), false),
                arguments(JAVA_OBJECT, ImmutableMap.of(), null, false),
                arguments(JAVA_OBJECT, ImmutableMap.of(), ImmutableMap.of(), true),
                arguments(JAVA_OBJECT, ImmutableMap.of("a", "B"), ImmutableMap.of(), false),
                arguments(JAVA_OBJECT, ImmutableMap.of("a", "B"), ImmutableMap.of("a", "b"), false),
                arguments(JAVA_OBJECT, ImmutableMap.of("a", "B"), ImmutableMap.of("x", "y"), false),
                arguments(JAVA_OBJECT, ImmutableMap.of(), ImmutableMap.of("x", "y"), false));
    }

    @ParameterizedTest
    @MethodSource("comparatorWithToleranceData")
    public void queryResultValueComparatorWithTolerance(JDBCType type, Object actual, Object expected, boolean result)
    {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getDouble(any())).thenReturn(Optional.of(Double.valueOf(0.01)));

        assertThat(QueryResultValueComparator.comparatorForType(type, configuration).test(actual, expected)).isEqualTo(result);
    }

    static Stream<Arguments> comparatorWithToleranceData()
    {
        return Stream.of(
                arguments(DOUBLE, Double.valueOf(1.0), Double.valueOf(1.0), true),
                arguments(DOUBLE, Double.valueOf(1.009999), Double.valueOf(1.0), true),
                arguments(DOUBLE, Double.valueOf(1.01), Double.valueOf(1.0), false),
                arguments(FLOAT, Double.valueOf(1.0), Double.valueOf(1.0), true),
                arguments(FLOAT, Double.valueOf(1.009999), Double.valueOf(1.0), true),
                arguments(FLOAT, Double.valueOf(1.01), Double.valueOf(1.0), false),

                arguments(DOUBLE, Double.valueOf(1000.0), Double.valueOf(1000.0), true),
                arguments(DOUBLE, Double.valueOf(1010.0), Double.valueOf(1000.0), true),
                arguments(DOUBLE, Double.valueOf(1010.001), Double.valueOf(1000.0), false),
                arguments(FLOAT, Double.valueOf(1000.0), Double.valueOf(1000.0), true),
                arguments(FLOAT, Double.valueOf(1010.0), Double.valueOf(1000.0), true),
                arguments(FLOAT, Double.valueOf(1010.001), Double.valueOf(1000.0), false),

                arguments(DOUBLE, Double.valueOf(-1000.0), Double.valueOf(-1000.0), true),
                arguments(DOUBLE, Double.valueOf(-1010.0), Double.valueOf(-1000.0), true),
                arguments(DOUBLE, Double.valueOf(-1010.001), Double.valueOf(-1000.0), false),
                arguments(FLOAT, Double.valueOf(-1000.0), Double.valueOf(-1000.0), true),
                arguments(FLOAT, Double.valueOf(-1010.0), Double.valueOf(-1000.0), true),
                arguments(FLOAT, Double.valueOf(-1010.001), Double.valueOf(-1000.0), false),

                arguments(DOUBLE, Double.NaN, Double.NaN, true),
                arguments(DOUBLE, Double.valueOf(0.001), Double.NaN, false),
                arguments(DOUBLE, Double.NaN, Double.valueOf(0.001), false),

                arguments(FLOAT, Float.NaN, Float.NaN, true),
                arguments(FLOAT, Float.valueOf(0.001f), Float.NaN, false),
                arguments(FLOAT, Float.NaN, Float.valueOf(0.001f), false));
    }

    @ParameterizedTest
    @MethodSource("comparatorThrowsData")
    public void queryResultValueComparatorThrowsIllegalArgumentException(JDBCType type, Object actual, Object expected)
    {
        Configuration configuration = mock(Configuration.class);

        assertThatThrownBy(() -> QueryResultValueComparator.comparatorForType(type, configuration).test(actual, expected))
                .isInstanceOf(IllegalArgumentException.class);
    }

    static Stream<Arguments> comparatorThrowsData()
    {
        return Stream.of(
                arguments(VARCHAR, "b", 1),
                arguments(BINARY, byteArray(0), 0),
                arguments(BIT, false, 0),
                arguments(BIGINT, 0L, "a"),
                arguments(DOUBLE, Double.valueOf(0.0), "a"),
                arguments(NUMERIC, BigDecimal.valueOf(0.0), "a"),
                arguments(DATE, Date.valueOf("2015-02-15"), "a"),
                arguments(TIME, Time.valueOf("10:10:10"), "a"),
                arguments(TIMESTAMP, Timestamp.valueOf("2015-02-15 10:10:10"), "a"));
    }

    private static byte[] byteArray(int value)
    {
        return new byte[] {(byte) value};
    }
}
