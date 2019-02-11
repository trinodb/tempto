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

package io.prestosql.tempto.internal.fulfillment.ldap;

import com.google.inject.Inject;
import io.prestosql.tempto.Requirement;
import io.prestosql.tempto.context.State;
import io.prestosql.tempto.fulfillment.RequirementFulfiller;
import io.prestosql.tempto.fulfillment.TestStatus;
import io.prestosql.tempto.fulfillment.ldap.LdapObjectRequirement;

import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;

@RequirementFulfiller.AutoSuiteLevelFulfiller
public class LdapObjectFulfiller<T extends LdapObjectRequirement>
        implements RequirementFulfiller
{
    @Inject
    LdapObjectEntryManager ldapObjectEntryManager;

    @Override
    public Set<State> fulfill(Set<Requirement> requirements)
    {
        requirements.stream()
                .filter(LdapObjectRequirement.class::isInstance)
                .map(LdapObjectRequirement.class::cast)
                .map(requirement -> requirement.getLdapObjectDefinitions())
                .distinct()
                .forEach(ldapObjectEntryManager::addLdapDefinitions);

        return newHashSet();
    }

    @Override
    public void cleanup(TestStatus status)
    {
    }
}
