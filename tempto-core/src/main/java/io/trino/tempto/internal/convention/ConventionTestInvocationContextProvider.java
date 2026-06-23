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

import io.trino.tempto.internal.listeners.TestNameGroupNameFilter;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

import java.util.List;
import java.util.stream.Stream;

/**
 * Turns the file-driven convention tests into native JUnit {@code @TestTemplate} invocations. Each
 * discovered test becomes one invocation that runs through the standard Tempto extension lifecycle
 * (see {@link ConventionTestExtension}), so convention tests behave exactly like regular product
 * tests. Name/group selection uses the same rules as regular tests ({@link TestNameGroupNameFilter}).
 */
public class ConventionTestInvocationContextProvider
        implements TestTemplateInvocationContextProvider
{
    @Override
    public boolean supportsTestTemplate(ExtensionContext context)
    {
        return true;
    }

    @Override
    public boolean mayReturnZeroTestTemplateInvocationContexts(ExtensionContext context)
    {
        // There may legitimately be no convention tests on the classpath.
        return true;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context)
    {
        return ConventionBasedTestFactory.selectedConventionTests(TestNameGroupNameFilter.fromSystemProperties())
                .stream()
                .map(ConventionTestInvocationContext::new);
    }

    private static final class ConventionTestInvocationContext
            implements TestTemplateInvocationContext
    {
        private final ConventionBasedTest conventionTest;

        private ConventionTestInvocationContext(ConventionBasedTest conventionTest)
        {
            this.conventionTest = conventionTest;
        }

        @Override
        public String getDisplayName(int invocationIndex)
        {
            return conventionTest.getTestName();
        }

        @Override
        public List<Extension> getAdditionalExtensions()
        {
            return List.of(new ConventionTestExtension(conventionTest));
        }
    }
}
