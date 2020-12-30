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

package io.trino.tempto.internal.fulfillment.resources;

import com.google.common.collect.ImmutableSet;
import io.trino.tempto.Requirement;
import io.trino.tempto.context.State;
import io.trino.tempto.fulfillment.RequirementFulfiller;
import io.trino.tempto.fulfillment.TestStatus;

import java.util.Set;

@RequirementFulfiller.TestLevelFulfiller
public class TestResourceFulfiller
        implements RequirementFulfiller
{
    private final TestResourcesState resourcesState = new TestResourcesState();

    @Override
    public Set<State> fulfill(Set<Requirement> requirements)
    {
        return ImmutableSet.of(resourcesState);
    }

    @Override
    public void cleanup(TestStatus status)
    {
        resourcesState.close();
    }
}
