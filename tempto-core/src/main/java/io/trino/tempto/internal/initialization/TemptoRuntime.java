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
package io.trino.tempto.internal.initialization;

import com.google.common.collect.ImmutableList;
import io.trino.tempto.TemptoPlugin;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.fulfillment.RequirementFulfiller;
import io.trino.tempto.fulfillment.RequirementFulfiller.SuiteLevelFulfiller;
import io.trino.tempto.fulfillment.RequirementFulfiller.TestLevelFulfiller;
import io.trino.tempto.initialization.SuiteModuleProvider;
import io.trino.tempto.initialization.TestMethodModuleProvider;
import io.trino.tempto.internal.ReflectionHelper;

import java.util.List;
import java.util.ServiceLoader;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.tempto.internal.configuration.TestConfigurationFactory.testConfiguration;
import static java.util.Objects.requireNonNull;

/**
 * Holds everything that is contributed by {@link TemptoPlugin}s (discovered via {@link ServiceLoader})
 * together with the resolved test {@link Configuration}. A single instance is created once per suite
 * and shared between the suite-level initialization and the per-test {@link TemptoTestExtension}.
 */
public class TemptoRuntime
{
    private final List<? extends SuiteModuleProvider> suiteModuleProviders;
    private final List<? extends TestMethodModuleProvider> testMethodModuleProviders;
    private final List<Class<? extends RequirementFulfiller>> suiteLevelFulfillers;
    private final List<Class<? extends RequirementFulfiller>> testMethodLevelFulfillers;
    private final Configuration configuration;

    public static TemptoRuntime createRuntime()
    {
        return createRuntime(testConfiguration());
    }

    public static TemptoRuntime createRuntime(Configuration configuration)
    {
        List<TemptoPlugin> plugins = ImmutableList.copyOf(ServiceLoader.load(TemptoPlugin.class).iterator());
        return new TemptoRuntime(
                plugins.stream()
                        .flatMap(plugin -> plugin.getSuiteModules().stream())
                        .map(ReflectionHelper::instantiate)
                        .collect(toImmutableList()),
                plugins.stream()
                        .flatMap(plugin -> plugin.getTestModules().stream())
                        .map(ReflectionHelper::instantiate)
                        .collect(toImmutableList()),
                plugins.stream()
                        .flatMap(plugin -> plugin.getFulfillers().stream())
                        .filter(clazz -> clazz.isAnnotationPresent(SuiteLevelFulfiller.class))
                        .collect(toImmutableList()),
                plugins.stream()
                        .flatMap(plugin -> plugin.getFulfillers().stream())
                        .filter(clazz -> clazz.isAnnotationPresent(TestLevelFulfiller.class))
                        .collect(toImmutableList()),
                configuration);
    }

    public TemptoRuntime(
            List<? extends SuiteModuleProvider> suiteModuleProviders,
            List<? extends TestMethodModuleProvider> testMethodModuleProviders,
            List<Class<? extends RequirementFulfiller>> suiteLevelFulfillers,
            List<Class<? extends RequirementFulfiller>> testMethodLevelFulfillers,
            Configuration configuration)
    {
        this.suiteModuleProviders = ImmutableList.copyOf(requireNonNull(suiteModuleProviders, "suiteModuleProviders is null"));
        this.testMethodModuleProviders = ImmutableList.copyOf(requireNonNull(testMethodModuleProviders, "testMethodModuleProviders is null"));
        this.suiteLevelFulfillers = ImmutableList.copyOf(requireNonNull(suiteLevelFulfillers, "suiteLevelFulfillers is null"));
        this.testMethodLevelFulfillers = ImmutableList.copyOf(requireNonNull(testMethodLevelFulfillers, "testMethodLevelFulfillers is null"));
        this.configuration = requireNonNull(configuration, "configuration is null");
    }

    public List<? extends SuiteModuleProvider> getSuiteModuleProviders()
    {
        return suiteModuleProviders;
    }

    public List<? extends TestMethodModuleProvider> getTestMethodModuleProviders()
    {
        return testMethodModuleProviders;
    }

    public List<Class<? extends RequirementFulfiller>> getSuiteLevelFulfillers()
    {
        return suiteLevelFulfillers;
    }

    public List<Class<? extends RequirementFulfiller>> getTestMethodLevelFulfillers()
    {
        return testMethodLevelFulfillers;
    }

    public Configuration getConfiguration()
    {
        return configuration;
    }
}
