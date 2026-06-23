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

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Exposes the convention-based (file driven) tests to JUnit. Each generated test runs as one
 * {@code @TestTemplate} invocation provided by {@link ConventionTestInvocationContextProvider}; the
 * Tempto per-test lifecycle (context, requirement fulfillment, member injection) is applied by the
 * standard extension mechanism, identically to regular product tests.
 */
public class ConventionBasedTests
{
    @TestTemplate
    @ExtendWith(ConventionTestInvocationContextProvider.class)
    void conventionBasedTest(ConventionBasedTest conventionTest)
    {
        conventionTest.test();
    }
}
