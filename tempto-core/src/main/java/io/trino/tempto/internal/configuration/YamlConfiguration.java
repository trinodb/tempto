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
import org.apache.commons.io.IOUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class YamlConfiguration
        extends DelegateConfiguration
{
    private final MapConfiguration mapConfiguration;

    public YamlConfiguration(String yamlString)
    {
        this(new ByteArrayInputStream(yamlString.getBytes(UTF_8)));
    }

    public YamlConfiguration(InputStream yamlInputStream)
    {
        mapConfiguration = loadConfiguration(inputStreamToStringSafe(yamlInputStream));
    }

    YamlConfiguration(MapConfiguration mapConfiguration)
    {
        this.mapConfiguration = mapConfiguration;
    }

    public static List<YamlConfiguration> loadAll(String yamlString)
    {
        return loadAll(new ByteArrayInputStream(yamlString.getBytes(UTF_8)));
    }

    public static List<YamlConfiguration> loadAll(InputStream yamlInputStream)
    {
        requireNonNull(yamlInputStream, "yamlInputStream is null");
        String yamlString = inputStreamToStringSafe(yamlInputStream);
        Yaml yaml = new Yaml();
        Iterable<Object> documents = yaml.loadAll(yamlString);
        return StreamSupport.stream(documents.spliterator(), false)
                .map(document -> new YamlConfiguration(buildMap(document)))
                .collect(Collectors.toList());
    }

    private MapConfiguration loadConfiguration(String yamlString)
    {
        requireNonNull(yamlString, "yamlString is null");
        Yaml yaml = new Yaml();
        Object loadedYaml = yaml.load(yamlString);
        return buildMap(loadedYaml);
    }

    private static MapConfiguration buildMap(Object document)
    {
        if (document == null) {
            // Empty input, or only comments
            return new MapConfiguration(ImmutableMap.of());
        }
        checkArgument(document instanceof Map, "yaml does not evaluate to map object; got %s", document.getClass().getName());
        Map<String, Object> loadedYamlMap = (Map<String, Object>) document;
        return new MapConfiguration(loadedYamlMap);
    }

    @Override
    protected Configuration getDelegate()
    {
        return mapConfiguration;
    }

    private static String inputStreamToStringSafe(InputStream yamlInputStream)
    {
        try {
            return IOUtils.toString(yamlInputStream, UTF_8);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
