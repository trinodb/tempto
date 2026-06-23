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
package io.trino.tempto.internal.query;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.JDBCType;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class QueryRowMapperTest
{
    @ParameterizedTest
    @MethodSource("convertBinaryDataProvider")
    public void convertBinary(String value, byte[] expected)
    {
        QueryRowMapper rowMapper = new QueryRowMapper(ImmutableList.of(JDBCType.BINARY));

        assertThat(rowMapper.mapToRow(ImmutableList.of(value)).getValues().get(0)).isEqualTo(expected);
    }

    static Stream<Arguments> convertBinaryDataProvider()
    {
        return Stream.of(
                Arguments.of("0000", bytes(0x00, 0x00)),
                Arguments.of("0ab0", bytes(0x0a, 0xb0)));
    }

    @Test
    public void shouldFailWhenIncorrectHex()
    {
        QueryRowMapper rowMapper = new QueryRowMapper(ImmutableList.of(JDBCType.BINARY));

        assertThatThrownBy(() -> rowMapper.mapToRow(ImmutableList.of("1a0")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static byte[] bytes(int... bytes)
    {
        byte[] result = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            result[i] = (byte) bytes[i];
        }
        return result;
    }
}
