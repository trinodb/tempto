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
package io.prestosql.tempto;

import com.google.common.collect.ImmutableList;
import io.prestosql.tempto.fulfillment.RequirementFulfiller;
import io.prestosql.tempto.fulfillment.table.TableDefinition;
import io.prestosql.tempto.fulfillment.table.TableManager;
import io.prestosql.tempto.initialization.SuiteModuleProvider;
import io.prestosql.tempto.initialization.TestMethodModuleProvider;

import java.util.List;

public interface TemptoPlugin
{
    default List<Class<? extends RequirementFulfiller>> getFulfillers()
    {
        return ImmutableList.of();
    }

    default List<Class<? extends SuiteModuleProvider>> getSuiteModules()
    {
        return ImmutableList.of();
    }

    default List<Class<? extends TestMethodModuleProvider>> getTestModules()
    {
        return ImmutableList.of();
    }

    default List<Class<? extends TableManager>> getTableManagers()
    {
        return ImmutableList.of();
    }

    default List<TableDefinition> getTables()
    {
        return ImmutableList.of();
    }
}
