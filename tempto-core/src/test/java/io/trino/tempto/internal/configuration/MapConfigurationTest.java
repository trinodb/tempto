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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static com.google.common.collect.Sets.newHashSet;
import static io.trino.tempto.internal.configuration.EmptyConfiguration.emptyConfiguration;
import static org.assertj.core.api.Assertions.assertThat;

public class MapConfigurationTest
{
    private MapConfiguration configuration;

    @BeforeEach
    public void setup()
    {
        configuration = new MapConfiguration(ImmutableMap.of(
                "a", ImmutableMap.of(
                        "b", ImmutableMap.of(
                                "c", "ala",
                                "d", "ela"),
                        "e", "tola",
                        "list1", Arrays.asList("element1", "element2", "element3")),
                "x", ImmutableMap.of(
                        "y", 10),
                "list2", Arrays.asList("element1", "element2", "element3")));
    }

    @Test
    public void testListKeys()
    {
        assertThat(configuration.listKeys()).isEqualTo(newHashSet("a.b.c", "a.b.d", "a.e", "x.y", "list2", "a.list1"));
    }

    @Test
    public void testGetObjects()
    {
        assertThat(configuration.get("a.b.c")).isEqualTo(Optional.of("ala"));
        assertThat(configuration.get("a.b.d")).isEqualTo(Optional.of("ela"));
        assertThat(configuration.get("x.y")).isEqualTo(Optional.of(10));
        assertThat(configuration.get("x.y.x")).isEqualTo(Optional.empty());
        assertThat(configuration.get("a.list1")).isEqualTo(Optional.of(Arrays.asList("element1", "element2", "element3")));
        assertThat(configuration.get("list2")).isEqualTo(Optional.of(Arrays.asList("element1", "element2", "element3")));
    }

    @Test
    public void testSubconfiguration()
    {
        var subConfigurationA = configuration.getSubconfiguration("a");
        var subConfigurationAB = configuration.getSubconfiguration("a.b");
        var subConfigurationX = configuration.getSubconfiguration("x");
        var subConfigurationXY = configuration.getSubconfiguration("x.y");

        assertThat(subConfigurationA.listKeys()).isEqualTo(newHashSet("b.c", "b.d", "e", "list1"));
        assertThat(subConfigurationAB.listKeys()).isEqualTo(newHashSet("c", "d"));
        assertThat(subConfigurationA.getString("b.c")).isEqualTo(Optional.of("ala"));
        assertThat(subConfigurationA.getString("b.d")).isEqualTo(Optional.of("ela"));
        assertThat(subConfigurationAB.getString("c")).isEqualTo(Optional.of("ala"));
        assertThat(subConfigurationAB.getString("d")).isEqualTo(Optional.of("ela"));
        assertThat(subConfigurationA.listPrefixes()).isEqualTo(newHashSet("b", "e", "list1"));
        assertThat(subConfigurationX.listPrefixes()).isEqualTo(newHashSet("y"));
        assertThat(subConfigurationX.getSubconfiguration("y")).isEqualTo(emptyConfiguration());
        assertThat(subConfigurationXY.listPrefixes()).isEqualTo(Collections.emptySet());
        assertThat(subConfigurationA.isList("e")).isFalse();
        assertThat(subConfigurationA.getString("e")).isEqualTo(Optional.of("tola"));
        assertThat(subConfigurationA.getStringOrList("e")).isEqualTo(Arrays.asList("tola"));
        assertThat(subConfigurationA.isList("list1")).isTrue();
        assertThat(subConfigurationA.getStringList("list1")).isEqualTo(Arrays.asList("element1", "element2", "element3"));
        assertThat(subConfigurationA.getStringOrList("list1")).isEqualTo(Arrays.asList("element1", "element2", "element3"));
    }
}
