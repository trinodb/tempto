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

package io.trino.tempto.internal.configuration;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static io.trino.tempto.configuration.KeyUtils.getKeyPrefix;
import static io.trino.tempto.configuration.KeyUtils.joinKey;
import static io.trino.tempto.configuration.KeyUtils.splitKey;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

public class KeyUtilsTest
{
    @Test
    public void testSplitKey()
    {
        assertThat(splitKey("abc")).isEqualTo(singletonList("abc"));
        assertThat(splitKey("a.b.c")).isEqualTo(Arrays.asList("a", "b", "c"));
    }

    @Test
    public void joinKeyTest()
    {
        assertThat(joinKey(Arrays.asList("a", "b", "c"))).isEqualTo("a.b.c");
        assertThat(joinKey(Arrays.asList("a", null, "c"))).isEqualTo("a.c");
        assertThat(joinKey(Arrays.asList(null, "b", "c"))).isEqualTo("b.c");
        assertThat(joinKey("a", "b", "c")).isEqualTo("a.b.c");
    }

    @Test
    public void getKeyPrefixTest()
    {
        assertThat(getKeyPrefix("a.b.c", 1)).isEqualTo("a");
        assertThat(getKeyPrefix("a.b.c", 2)).isEqualTo("a.b");
        assertThat(getKeyPrefix("a.b.c", 3)).isEqualTo("a.b.c");
        assertThat(getKeyPrefix("a.b.c", 4)).isEqualTo("a.b.c");
    }
}
