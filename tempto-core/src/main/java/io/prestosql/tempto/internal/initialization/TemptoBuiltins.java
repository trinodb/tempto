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
package io.prestosql.tempto.internal.initialization;

import com.google.common.collect.ImmutableList;
import io.prestosql.tempto.TemptoPlugin;
import io.prestosql.tempto.fulfillment.RequirementFulfiller;
import io.prestosql.tempto.fulfillment.table.ReadOnlyTableManager;
import io.prestosql.tempto.fulfillment.table.TableDefinition;
import io.prestosql.tempto.fulfillment.table.TableManager;
import io.prestosql.tempto.fulfillment.table.hive.tpcds.TpcdsTableDefinitions;
import io.prestosql.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions;
import io.prestosql.tempto.fulfillment.table.jdbc.tpch.JdbcTpchTableDefinitions;
import io.prestosql.tempto.initialization.SuiteModuleProvider;
import io.prestosql.tempto.initialization.TestMethodModuleProvider;
import io.prestosql.tempto.internal.configuration.TestConfigurationModuleProvider;
import io.prestosql.tempto.internal.fulfillment.command.SuiteCommandFulfiller;
import io.prestosql.tempto.internal.fulfillment.command.TestCommandFulfiller;
import io.prestosql.tempto.internal.fulfillment.table.ImmutableTablesFulfiller;
import io.prestosql.tempto.internal.fulfillment.table.MutableTablesFulfiller;
import io.prestosql.tempto.internal.fulfillment.table.TableManagerDispatcherModuleProvider;
import io.prestosql.tempto.internal.fulfillment.table.cassandra.CassandraTableManager;
import io.prestosql.tempto.internal.fulfillment.table.hive.HiveTableManager;
import io.prestosql.tempto.internal.fulfillment.table.jdbc.JdbcTableManager;
import io.prestosql.tempto.internal.hadoop.hdfs.HdfsModuleProvider;
import io.prestosql.tempto.internal.initialization.modules.TestMethodInfoModuleProvider;
import io.prestosql.tempto.internal.query.QueryExecutorModuleProvider;
import io.prestosql.tempto.internal.ssh.SshClientModuleProvider;

import java.util.List;

public class TemptoBuiltins
        implements TemptoPlugin
{
    @Override
    public List<Class<? extends RequirementFulfiller>> getFulfillers()
    {
        return ImmutableList.of(
                ImmutableTablesFulfiller.class,
                SuiteCommandFulfiller.class,
                MutableTablesFulfiller.class,
                TestCommandFulfiller.class);
    }

    @Override
    public List<Class<? extends SuiteModuleProvider>> getSuiteModules()
    {
        return ImmutableList.of(
                HdfsModuleProvider.class,
                QueryExecutorModuleProvider.class,
                SshClientModuleProvider.class,
                TableManagerDispatcherModuleProvider.class,
                TestConfigurationModuleProvider.class);
    }

    @Override
    public List<Class<? extends TestMethodModuleProvider>> getTestModules()
    {
        return ImmutableList.of(TestMethodInfoModuleProvider.class);
    }

    @Override
    public List<Class<? extends TableManager>> getTableManagers()
    {
        return ImmutableList.of(
                CassandraTableManager.class,
                HiveTableManager.class,
                JdbcTableManager.class,
                ReadOnlyTableManager.class);
    }

    @Override
    public List<TableDefinition> getTables()
    {
        return ImmutableList.of(
                TpcdsTableDefinitions.CALL_CENTER,
                TpcdsTableDefinitions.CATALOG_PAGE,
                TpcdsTableDefinitions.CATALOG_RETURNS,
                TpcdsTableDefinitions.CATALOG_SALES,
                TpcdsTableDefinitions.CUSTOMER,
                TpcdsTableDefinitions.CUSTOMER_ADDRESS,
                TpcdsTableDefinitions.CUSTOMER_DEMOGRAPHICS,
                TpcdsTableDefinitions.DATE_DIM,
                TpcdsTableDefinitions.HOUSEHOLD_DEMOGRAPHICS,
                TpcdsTableDefinitions.INCOME_BAND,
                TpcdsTableDefinitions.INVENTORY,
                TpcdsTableDefinitions.ITEM,
                TpcdsTableDefinitions.PROMOTION,
                TpcdsTableDefinitions.REASON,
                TpcdsTableDefinitions.SHIP_MODE,
                TpcdsTableDefinitions.STORE,
                TpcdsTableDefinitions.STORE_RETURNS,
                TpcdsTableDefinitions.STORE_SALES,
                TpcdsTableDefinitions.TIME_DIM,
                TpcdsTableDefinitions.WAREHOUSE,
                TpcdsTableDefinitions.WEB_PAGE,
                TpcdsTableDefinitions.WEB_RETURNS,
                TpcdsTableDefinitions.WEB_SALES,
                TpcdsTableDefinitions.WEB_SITE,

                TpchTableDefinitions.CUSTOMER,
                TpchTableDefinitions.LINE_ITEM,
                TpchTableDefinitions.NATION,
                TpchTableDefinitions.ORDERS,
                TpchTableDefinitions.PART,
                TpchTableDefinitions.PART_SUPPLIER,
                TpchTableDefinitions.REGION,
                TpchTableDefinitions.SUPPLIER,

                JdbcTpchTableDefinitions.NATION);
    }
}
