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

import io.trino.tempto.RequirementsProvider;
import io.trino.tempto.testmarkers.WithName;
import io.trino.tempto.testmarkers.WithTestGroups;

/**
 * A single convention-based (file driven) test. These are exposed to JUnit as dynamic tests by
 * {@link ConventionBasedTests}; the per-test Tempto lifecycle (context, requirement fulfillment,
 * member injection) is applied around {@link #test()} by the dynamic test executable.
 */
public abstract class ConventionBasedTest
        implements RequirementsProvider,
                   WithName,
                   WithTestGroups
{
    public abstract void test();
}
