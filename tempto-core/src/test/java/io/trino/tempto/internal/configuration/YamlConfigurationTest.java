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
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.Sets.newHashSet;
import static org.assertj.core.api.Assertions.assertThat;

public class YamlConfigurationTest
{
    @Test
    public void create()
    {
        YamlConfiguration configuration = new YamlConfiguration(
                """
                a:
                   b:
                      d: ela${foo}
                x:
                   y: 10
                list:
                    - element1
                    - element2
                """);

        assertThat(configuration.listKeys()).isEqualTo(newHashSet("a.b.d", "x.y", "list"));
        assertThat(configuration.getString("missing")).isEqualTo(Optional.empty());
        assertThat(configuration.getStringList("missing")).isEqualTo(Collections.emptyList());
        assertThat(configuration.getStringOrList("missing")).isEqualTo(Collections.emptyList());
        assertThat(configuration.getInt("x.y")).isEqualTo(Optional.of(10));
        assertThat(configuration.getString("a.b.d")).isEqualTo(Optional.of("ela${foo}"));
        assertThat(configuration.isList("x.y")).isFalse();
        assertThat(configuration.getString("x.y")).isEqualTo(Optional.of("10"));
        assertThat(configuration.getStringOrList("x.y")).isEqualTo(Arrays.asList("10"));
        assertThat(configuration.isList("list")).isTrue();
        assertThat(configuration.getStringList("list")).isEqualTo(Arrays.asList("element1", "element2"));
        assertThat(configuration.getStringOrList("list")).isEqualTo(Arrays.asList("element1", "element2"));
    }

    @Test
    public void createMultiple()
    {
        List<YamlConfiguration> configurations = YamlConfiguration.loadAll(
                """
                a: 1
                list:
                    - element1
                    - element2
                ---
                b: 2
                """);

        assertThat(configurations.get(0).listKeys()).isEqualTo(newHashSet("a", "list"));
        assertThat(configurations.get(0).getInt("a")).isEqualTo(Optional.of(1));
        assertThat(configurations.get(0).isList("list")).isTrue();
        assertThat(configurations.get(0).getStringList("list")).isEqualTo(Arrays.asList("element1", "element2"));
        assertThat(configurations.get(0).getStringOrList("list")).isEqualTo(Arrays.asList("element1", "element2"));
        assertThat(configurations.get(1).listKeys()).isEqualTo(newHashSet("b"));
        assertThat(configurations.get(1).getInt("b")).isEqualTo(Optional.of(2));
    }

    @Test
    public void createEmpty()
    {
        YamlConfiguration configuration = new YamlConfiguration("");

        assertThat(configuration.listKeys()).isEqualTo(Collections.emptySet());
        assertThat(configuration.getString("missing")).isEqualTo(Optional.empty());
    }

    @Test
    public void createFromAllComments()
    {
        YamlConfiguration configuration = new YamlConfiguration("# comment");

        assertThat(configuration.listKeys()).isEqualTo(Collections.emptySet());
        assertThat(configuration.getString("missing")).isEqualTo(Optional.empty());
    }
}
