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

package io.trino.tempto.internal

import com.google.common.collect.ImmutableList
import com.google.inject.AbstractModule
import com.google.inject.ConfigurationException
import com.google.inject.Inject
import com.google.inject.Injector
import com.google.inject.Key
import com.google.inject.TypeLiteral
import com.google.inject.name.Named
import io.trino.tempto.Requirement
import io.trino.tempto.context.State
import io.trino.tempto.context.TestContext
import io.trino.tempto.context.TestContextCloseCallback
import io.trino.tempto.fulfillment.RequirementFulfiller
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test

import java.lang.reflect.Method

import static com.google.inject.Guice.createInjector
import static com.google.inject.name.Names.named
import static org.junit.jupiter.api.Assertions.assertThrows

class ReflectionInjectorHelperTest
{
    private final ReflectionInjectorHelper reflectionInjectorHelper = new ReflectionInjectorHelper()
    private TestContext testContext;

    @BeforeEach
    void setup()
    {
        Injector injector = createInjector(new AbstractModule() {
            @Override
            protected void configure()
            {
                bind(Requirement).toInstance(new Requirement() {})
                bind(Key.get(String, named('key'))).toInstance('value')
                bind(Key.get(String, named('key2'))).toInstance('value2')

                List<String> strings = ImmutableList.of('ala', 'ma', 'kota')
                bind(new TypeLiteral<List<String>>() {}).toInstance(strings)
            }
        })
        testContext = new TestContext() {
            @Override
            def <T> T getDependency(Class<T> dependencyClass) {
                return null
            }

            @Override
            def <T> T getDependency(Class<T> dependencyClass, String dependencyName)
            {
                return null
            }

            @Override
            def <T> Optional<T> getOptionalDependency(Class<T> dependencyClass) {
                return null
            }

            @Override
            def <T> Optional<T> getOptionalDependency(Class<T> dependencyClass, String dependencyName) {
                return null
            }

            @Override
            TestContext createChildContext(Iterable<State> states) {
                return null
            }

            @Override
            void registerCloseCallback(TestContextCloseCallback callback) {
            }

            @Override
            void injectMembers(Object instance)
            {
                injector.injectMembers(instance)
            }

            @Override
            void close() {
            }
        }
    }

    @Test
    void canInjectRequirementToMethod()
    {
        injectAndCallMethod('useRequirement', Requirement)
    }

    private void injectAndCallMethod(String methodName, Class... parameterTypes)
    {
        Method method = getClass().getMethod(methodName, parameterTypes)

        method.invoke(this, reflectionInjectorHelper.getMethodArguments(testContext, method))
    }

    @Inject
    void useRequirement(Requirement requirement)
    {
        assert requirement != null
    }

    @Test
    void canInjectNamedStringToMethod()
    {
        injectAndCallMethod('useKey', String)
        injectAndCallMethod('useKey2', String)
    }

    @Inject
    void useKey(@Named('key') String key)
    {
        assert key == 'value';
    }

    @jakarta.inject.Inject
    void useKey2(@jakarta.inject.Named('key2') String key)
    {
        assert key == 'value2';
    }

    @Test
    @Disabled
    void canInjectStringListToMethod()
    {
        injectAndCallMethod('useStringList', List)
    }

    @Inject
    void useStringList(List<String> stringList)
    {
        assert stringList.size() == 3 && stringList.containsAll(['ala', 'ma', 'kota'])
    }

    @Test
    void cannotInjectRequirementFulfillerToMethod()
    {
        assertThrows(ConfigurationException, { _ -> injectAndCallMethod('useFulfiller', RequirementFulfiller) })
    }

    @Inject
    void useFulfiller(RequirementFulfiller fulfiller)
    {
        throw new IllegalStateException('no fulfiller should be provided')
    }
}
