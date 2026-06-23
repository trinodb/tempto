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

import com.google.common.collect.ImmutableMap;
import io.trino.tempto.configuration.Configuration;
import org.junit.jupiter.api.Test;

import static com.google.common.collect.Sets.newHashSet;
import static org.assertj.core.api.Assertions.assertThat;

public class HierarchicalConfigurationTest
{
    @Test
    public void testHierarchicalConfiguration()
    {
        Configuration a = new MapConfiguration(ImmutableMap.of(
                "a", "a",
                "ab", "a",
                "ac", "a",
                "abc", "a",
                "sub", ImmutableMap.of("ac", "a")));

        Configuration b = new MapConfiguration(ImmutableMap.of(
                "b", "b",
                "ab", "b",
                "bc", "b",
                "abc", "b"));

        Configuration c = new MapConfiguration(ImmutableMap.of(
                "c", "c",
                "ac", "c",
                "bc", "c",
                "abc", "c",
                "sub", ImmutableMap.of("ac", "c")));

        HierarchicalConfiguration configuration = new HierarchicalConfiguration(a, b, c);

        assertThat(configuration.getStringMandatory("a")).isEqualTo("a");
        assertThat(configuration.getStringMandatory("b")).isEqualTo("b");
        assertThat(configuration.getStringMandatory("c")).isEqualTo("c");
        assertThat(configuration.getStringMandatory("ab")).isEqualTo("b");
        assertThat(configuration.getStringMandatory("ac")).isEqualTo("c");
        assertThat(configuration.getStringMandatory("bc")).isEqualTo("c");
        assertThat(configuration.getStringMandatory("abc")).isEqualTo("c");
        assertThat(configuration.getStringMandatory("sub.ac")).isEqualTo("c");

        assertThat(configuration.getSubconfiguration("sub").getStringMandatory("ac")).isEqualTo("c");

        assertThat(configuration.listKeys()).isEqualTo(newHashSet("a", "b", "c", "ab", "ac", "bc", "abc", "sub.ac"));
    }
}
