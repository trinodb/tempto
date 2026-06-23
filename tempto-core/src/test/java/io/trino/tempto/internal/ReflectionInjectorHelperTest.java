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

package io.trino.tempto.internal;

import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.ConfigurationException;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;
import io.trino.tempto.Requirement;
import io.trino.tempto.context.TestContext;
import io.trino.tempto.fulfillment.RequirementFulfiller;
import io.trino.tempto.internal.context.GuiceTestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;

import static com.google.inject.name.Names.named;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ReflectionInjectorHelperTest
{
    private final ReflectionInjectorHelper reflectionInjectorHelper = new ReflectionInjectorHelper();
    private TestContext testContext;

    @BeforeEach
    public void setup()
    {
        testContext = new GuiceTestContext(new AbstractModule()
        {
            @Override
            protected void configure()
            {
                bind(Requirement.class).toInstance(new Requirement() {});
                bind(Key.get(String.class, named("key"))).toInstance("value");
                bind(Key.get(String.class, named("key2"))).toInstance("value2");

                List<String> strings = ImmutableList.of("ala", "ma", "kota");
                bind(new TypeLiteral<List<String>>() {}).toInstance(strings);
            }
        });
    }

    @Test
    public void canInjectRequirementToMethod()
            throws Exception
    {
        injectAndCallMethod("useRequirement", Requirement.class);
    }

    private void injectAndCallMethod(String methodName, Class<?>... parameterTypes)
            throws Exception
    {
        Method method = getClass().getMethod(methodName, parameterTypes);

        method.invoke(this, reflectionInjectorHelper.getMethodArguments(testContext, method));
    }

    @Inject
    public void useRequirement(Requirement requirement)
    {
        assertThat(requirement).isNotNull();
    }

    @Test
    public void canInjectNamedStringToMethod()
            throws Exception
    {
        injectAndCallMethod("useKey", String.class);
        injectAndCallMethod("useKey2", String.class);
    }

    @Inject
    public void useKey(@Named("key") String key)
    {
        assertThat(key).isEqualTo("value");
    }

    @jakarta.inject.Inject
    public void useKey2(@jakarta.inject.Named("key2") String key)
    {
        assertThat(key).isEqualTo("value2");
    }

    @Test
    @Disabled
    public void canInjectStringListToMethod()
            throws Exception
    {
        injectAndCallMethod("useStringList", List.class);
    }

    @Inject
    public void useStringList(List<String> stringList)
    {
        assertThat(stringList).containsExactlyInAnyOrder("ala", "ma", "kota");
    }

    @Test
    public void cannotInjectRequirementFulfillerToMethod()
    {
        assertThrows(ConfigurationException.class, () -> injectAndCallMethod("useFulfiller", RequirementFulfiller.class));
    }

    @Inject
    public void useFulfiller(RequirementFulfiller fulfiller)
    {
        throw new IllegalStateException("no fulfiller should be provided");
    }
}
