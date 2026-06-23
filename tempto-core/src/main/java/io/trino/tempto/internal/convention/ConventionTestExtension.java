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
package io.trino.tempto.internal.convention;

import io.trino.tempto.initialization.TestMethodInfo;
import io.trino.tempto.internal.initialization.AbstractTemptoTestExtension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.lang.reflect.Method;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Drives the Tempto per-test lifecycle for a single convention-based test and supplies it to the
 * {@code @TestTemplate} method as a parameter. One instance is registered per template invocation by
 * {@link ConventionTestInvocationContextProvider}, so the convention test is known up front and no
 * cross-extension state passing is needed.
 */
class ConventionTestExtension
        extends AbstractTemptoTestExtension
        implements ParameterResolver
{
    private static final Method TEST_METHOD = testMethod();

    private final ConventionBasedTest conventionTest;

    ConventionTestExtension(ConventionBasedTest conventionTest)
    {
        this.conventionTest = requireNonNull(conventionTest, "conventionTest is null");
    }

    @Override
    protected TestMethodInfo testMethodInfo(ExtensionContext context)
    {
        return new TestMethodInfo(
                conventionTest.getTestName(),
                conventionTest.getTestGroups(),
                conventionTest.getClass(),
                Optional.of(TEST_METHOD),
                conventionTest);
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
    {
        return parameterContext.getParameter().getType().equals(ConventionBasedTest.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
    {
        return conventionTest;
    }

    private static Method testMethod()
    {
        try {
            return ConventionBasedTest.class.getMethod("test");
        }
        catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
    }
}
