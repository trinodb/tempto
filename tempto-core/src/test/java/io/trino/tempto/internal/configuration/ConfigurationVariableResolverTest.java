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

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ConfigurationVariableResolverTest
{
    private static final String ENV_VARIABLE_KEY = System.getenv().keySet().iterator().next();

    private final ConfigurationVariableResolver resolver = new ConfigurationVariableResolver();

    @Test
    public void resolveSystemEnv()
    {
        Configuration configuration = new MapConfiguration(ImmutableMap.of(
                "variable", "${" + ENV_VARIABLE_KEY + "}"));

        configuration = resolver.resolve(configuration);

        assertThat(configuration.getStringMandatory("variable")).isEqualTo(System.getenv(ENV_VARIABLE_KEY));
    }

    @Test
    public void resolveSystemEnvHasHigherPriorityThanFromConfiguration()
    {
        Configuration configuration = new MapConfiguration(ImmutableMap.of(
                "variable", "${" + ENV_VARIABLE_KEY + "}",
                ENV_VARIABLE_KEY, "value from configuration"));

        configuration = resolver.resolve(configuration);

        assertThat(configuration.getStringMandatory("variable")).isEqualTo(System.getenv(ENV_VARIABLE_KEY));
    }

    @Test
    public void resolveSystemEnvHasHigherPriorityThanFromSystemProperties()
    {
        System.setProperty(ENV_VARIABLE_KEY, "value from system properties");
        Configuration configuration = new MapConfiguration(ImmutableMap.of(
                "variable", "${" + ENV_VARIABLE_KEY + "}",
                ENV_VARIABLE_KEY, "value from configuration"));

        configuration = resolver.resolve(configuration);

        assertThat(configuration.getStringMandatory("variable")).isEqualTo(System.getenv(ENV_VARIABLE_KEY));
    }

    @Test
    public void resolveSystemPropertiesHasHigherPriorityThanFromConfiguration()
    {
        String key = "SYSTEM_PROPERTY";
        String valueFromSystemProperties = "value from system properties";
        System.setProperty(key, valueFromSystemProperties);

        Configuration configuration = new MapConfiguration(ImmutableMap.of(
                "variable", "${" + key + "}",
                key, "value from configuration"));

        configuration = resolver.resolve(configuration);

        assertThat(configuration.getStringMandatory("variable")).isEqualTo(valueFromSystemProperties);
    }

    @Test
    public void resolveConfigurationVariables()
    {
        Configuration configuration = new MapConfiguration(ImmutableMap.of(
                "items", ImmutableMap.of(
                        "who", "ala",
                        "verb", "ma",
                        "what", "kota",
                        "what_alias", "${items.what}"),
                "story", "${items.who} ${items.verb} ${items.what}",
                "story_with_alias", "${items.who} ${items.verb} ${items.what_alias}"));

        configuration = resolver.resolve(configuration);

        assertThat(configuration.getStringMandatory("story")).isEqualTo("ala ma kota");
        assertThat(configuration.getStringMandatory("story_with_alias")).isEqualTo("ala ma kota");
    }

    @Test
    public void resolveConfigurationListVariables()
    {
        Configuration configuration = new MapConfiguration(ImmutableMap.of(
                "items", ImmutableMap.of(
                        "who", "ala",
                        "verb", "ma",
                        "what", "kota",
                        "int", 1),
                "list_alias", Arrays.asList("${items.who}", "${items.what}", "${items.int}")));

        configuration = resolver.resolve(configuration);

        assertThat(configuration.getStringList("list_alias")).isEqualTo(Arrays.asList("ala", "kota", "1"));
    }

    @Test
    public void unableToResolveWhenCyclicReferences()
    {
        Configuration configuration = new MapConfiguration(ImmutableMap.of(
                "first", "${second}",
                "second", "${third}",
                "third", "${first}"));

        assertThatThrownBy(() -> resolver.resolve(configuration))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Infinite loop in property interpolation of ${first}: first->second->third");
    }

    @Test
    public void unableToResolveUnknownVariables()
    {
        Configuration configuration = new MapConfiguration(ImmutableMap.of(
                "first", "${second}"));

        assertThat(configuration.getStringMandatory("first")).isEqualTo("${second}");
    }

    @Test
    public void typesAreNotLost()
    {
        Configuration configuration = new MapConfiguration(ImmutableMap.of(
                "int", 1,
                "int_alias", "${int}",
                "boolean", false,
                "boolean_alias", "${boolean}"));

        configuration = resolver.resolve(configuration);

        assertThat(configuration.getIntMandatory("int")).isEqualTo(1);
        assertThat(configuration.getIntMandatory("int_alias")).isEqualTo(1);
        assertThat(configuration.getBooleanMandatory("boolean")).isFalse();
        assertThat(configuration.getBooleanMandatory("boolean_alias")).isFalse();
    }
}
