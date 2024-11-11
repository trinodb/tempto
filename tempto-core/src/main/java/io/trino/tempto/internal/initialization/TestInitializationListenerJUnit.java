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
import com.google.common.collect.Ordering;
import com.google.inject.Module;
import io.trino.tempto.TemptoPlugin;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.context.TestContext;
import io.trino.tempto.fulfillment.table.TableManager;
import io.trino.tempto.fulfillment.table.TableManagerDispatcher;
import io.trino.tempto.initialization.SuiteModuleProvider;
import io.trino.tempto.internal.ReflectionHelper;
import io.trino.tempto.internal.context.GuiceTestContext;
import io.trino.tempto.internal.context.TestContextStack;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;

import java.util.List;
import java.util.ServiceLoader;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.inject.util.Modules.combine;
import static io.trino.tempto.context.ThreadLocalTestContextHolder.assertTestContextNotSet;
import static io.trino.tempto.context.ThreadLocalTestContextHolder.popAllTestContexts;
import static io.trino.tempto.context.ThreadLocalTestContextHolder.pushAllTestContexts;
import static io.trino.tempto.context.ThreadLocalTestContextHolder.testContextIfSet;
import static io.trino.tempto.internal.configuration.TestConfigurationFactory.testConfiguration;
import static org.slf4j.LoggerFactory.getLogger;

public class TestInitializationListenerJUnit
        implements
        BeforeAllCallback,
        AfterAllCallback
{
    private static final Logger LOGGER = getLogger(TestInitializationListenerJUnit.class);

    private final List<? extends SuiteModuleProvider> suiteModuleProviders;

    private final Configuration configuration;

    public TestInitializationListenerJUnit()
    {
        this(ImmutableList.copyOf(ServiceLoader.load(TemptoPlugin.class).iterator()));
    }

    private TestInitializationListenerJUnit(List<TemptoPlugin> plugins)
    {
        this(
                plugins.stream()
                        .flatMap(plugin -> plugin.getSuiteModules().stream())
                        .map(ReflectionHelper::instantiate)
                        .collect(toImmutableList()),
                testConfiguration());
    }

    TestInitializationListenerJUnit(
            List<? extends SuiteModuleProvider> suiteModuleProviders,
            Configuration configuration)
    {
        this.suiteModuleProviders = suiteModuleProviders;
        this.configuration = configuration;
    }

    @Override
    public void beforeAll(ExtensionContext context)
    {
        displayConfigurationToUser();

        Module suiteModule = combine(getSuiteModules());
        GuiceTestContext initSuiteTestContext = new GuiceTestContext(suiteModule);
        TestContextStack<TestContext> suiteTextContextStack = new TestContextStack<>();
        suiteTextContextStack.push(initSuiteTestContext);
        assertTestContextNotSet();
        pushAllTestContexts(suiteTextContextStack);
        TestContext topTestContext = suiteTextContextStack.peek();
        topTestContext.injectMembers(context.getRequiredTestInstance());
    }

    private void displayConfigurationToUser()
    {
        LOGGER.info("Configuration:");
        List<String> configurationKeys = Ordering.natural()
                .sortedCopy(configuration.listKeys());
        for (String key : configurationKeys) {
            LOGGER.info(String.format("%s -> %s", key, configuration.getString(key).orElse("<NOT SET>")));
        }
    }

    @Override
    public void afterAll(ExtensionContext context)
    {
        if (testContextIfSet().isEmpty()) {
            throw new IllegalStateException("Test context at this point may not be initialized only because of exception during initialization");
        }

        TestContextStack<TestContext> testContextStack = popAllTestContexts();
        // we are going to close last context, so we need to close TableManager's first
        testContextStack.peek().getOptionalDependency(TableManagerDispatcher.class)
                .ifPresent(dispatcher -> dispatcher.getAllTableManagers().forEach(TableManager::close));

        // remove close init test context too
        testContextStack.peek().close();
    }

    private List<Module> getSuiteModules()
    {
        return suiteModuleProviders
                .stream()
                .map(provider -> provider.getModule(configuration))
                .collect(toImmutableList());
    }
}
