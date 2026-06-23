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

import io.trino.tempto.initialization.TestMethodInfo;
import io.trino.tempto.testmarkers.WithName;
import io.trino.tempto.testmarkers.WithTestGroups;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

/**
 * JUnit 5 extension registered (via {@code @ExtendWith} on {@link io.trino.tempto.ProductTest}) on
 * every Tempto product test. The lifecycle lives in {@link AbstractTemptoTestExtension}; this class
 * only describes the test from the JUnit {@link ExtensionContext}.
 */
public class TemptoTestExtension
        extends AbstractTemptoTestExtension
{
    @Override
    protected TestMethodInfo testMethodInfo(ExtensionContext context)
    {
        Class<?> testClass = context.getRequiredTestClass();
        Object testInstance = context.getRequiredTestInstance();
        return new TestMethodInfo(
                readTestName(context, testInstance),
                readTestGroups(context, testInstance),
                testClass,
                Optional.of(context.getRequiredTestMethod()),
                testInstance);
    }

    private static String readTestName(ExtensionContext context, Object testInstance)
    {
        if (testInstance instanceof WithName withName) {
            return withName.getTestName();
        }
        return context.getRequiredTestClass().getName() + "." + context.getRequiredTestMethod().getName();
    }

    private static Set<String> readTestGroups(ExtensionContext context, Object testInstance)
    {
        Set<String> groups = new LinkedHashSet<>(context.getTags());
        if (testInstance instanceof WithTestGroups withTestGroups) {
            groups.addAll(withTestGroups.getTestGroups());
        }
        return groups;
    }
}
