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
package io.prestosql.tempto.fulfillment.table;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.tempto.fulfillment.table.hive.HiveDataSource;
import io.prestosql.tempto.fulfillment.table.hive.HiveTableDefinition;
import io.prestosql.tempto.fulfillment.table.hive.tpcds.TpcdsTableDefinitions;
import io.prestosql.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions;
import io.prestosql.tempto.fulfillment.table.jdbc.RelationalDataSource;
import io.prestosql.tempto.fulfillment.table.jdbc.tpch.JdbcTpchTableDefinitions;
import io.prestosql.tempto.internal.convention.tabledefinitions.ConventionTableDefinitionDescriptor;
import io.prestosql.tempto.internal.convention.tabledefinitions.FileBasedHiveDataSource;
import io.prestosql.tempto.internal.convention.tabledefinitions.FileBasedRelationalDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.tempto.fulfillment.table.TableHandle.tableHandle;
import static io.prestosql.tempto.fulfillment.table.hive.HiveTableDefinition.hiveTableDefinition;
import static io.prestosql.tempto.fulfillment.table.jdbc.RelationalTableDefinition.relationalTableDefinition;
import static io.prestosql.tempto.internal.convention.ConventionTestsUtils.getConventionsTestsPath;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.newDirectoryStream;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

/**
 * Stores {@link TableDefinition} mapped by names.
 */
public class TableDefinitionsRepository
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TableDefinitionsRepository.class);

    private static final String DATASETS_PATH_PART = "datasets";

    private final static List<TableDefinition> BUILTIN_TABLE_DEFINITIONS = ImmutableList.of(
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

    // TODO find better way to pass extensions
    public static Supplier<List<TableDefinition>> ADDITIONAL_TABLE_DEFINITIONS = () -> ImmutableList.of();

    private static TableDefinitionsRepository TABLE_DEFINITIONS_REPOSITORY;

    public static TableDefinition tableDefinition(TableHandle tableHandle)
    {
        return tableDefinitionsRepository().get(tableHandle);
    }

    public static TableDefinitionsRepository tableDefinitionsRepository()
    {
        if (TABLE_DEFINITIONS_REPOSITORY == null) {
            TABLE_DEFINITIONS_REPOSITORY = new TableDefinitionsRepository(ImmutableList.<TableDefinition>builder()
                    .addAll(BUILTIN_TABLE_DEFINITIONS)
                    .addAll(ADDITIONAL_TABLE_DEFINITIONS.get())
                    // TODO: since TestNG has no listener that can be run before tests factory, this has to be initialized here
                    .addAll(getAllConventionBasedTableDefinitions())
                    .build());
        }
        return TABLE_DEFINITIONS_REPOSITORY;
    }

    private final Map<TableDefinitionRepositoryKey, TableDefinition> tableDefinitions;

    private TableDefinitionsRepository(Collection<TableDefinition> tableDefinitions)
    {
        Map<TableDefinitionRepositoryKey, TableDefinition> definitions = new HashMap<>();
        for (TableDefinition tableDefinition : tableDefinitions) {
            TableDefinitionRepositoryKey repositoryKey = asRepositoryKey(tableDefinition.getTableHandle());
            checkState(!definitions.containsKey(repositoryKey), "duplicated table definition: %s", repositoryKey);
            definitions.put(repositoryKey, tableDefinition);
        }
        this.tableDefinitions = ImmutableMap.copyOf(definitions);
    }

    public TableDefinition get(TableHandle tableHandle)
    {
        TableDefinitionRepositoryKey tableHandleKey = asRepositoryKey(tableHandle);
        TableDefinitionRepositoryKey nameKey = asRepositoryKey(tableHandle.getName());
        if (tableDefinitions.containsKey(tableHandleKey)) {
            return tableDefinitions.get(tableHandleKey);
        }
        if (tableDefinitions.containsKey(nameKey)) {
            return tableDefinitions.get(nameKey);
        }
        throw new IllegalStateException("no table definition for: " + tableHandleKey);
    }

    private static List<TableDefinition> getAllConventionBasedTableDefinitions()
    {
        Optional<Path> dataSetsPath = getConventionsTestsPath(DATASETS_PATH_PART);
        if (!dataSetsPath.isPresent()) {
            LOGGER.debug("No convention table definitions");
            return emptyList();
        }
        return getAllConventionTableDefinitionDescriptors(dataSetsPath.get())
                .stream()
                .map(TableDefinitionsRepository::tableDefinitionFor)
                .collect(toImmutableList());
    }

    private static List<ConventionTableDefinitionDescriptor> getAllConventionTableDefinitionDescriptors(Path dataSetsPath)
    {
        if (exists(dataSetsPath)) {
            LOGGER.debug("Data sets configuration for path: {}", dataSetsPath);

            try {
                return StreamSupport.stream(newDirectoryStream(dataSetsPath, "*.ddl").spliterator(), false)
                        .map(ddlFile -> new ConventionTableDefinitionDescriptor(ddlFile))
                        .collect(toImmutableList());
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return emptyList();
    }

    private static TableDefinition tableDefinitionFor(ConventionTableDefinitionDescriptor tableDefinitionDescriptor)
    {
        ConventionTableDefinitionDescriptor.ParsedDDLFile parsedDDLFile = tableDefinitionDescriptor.getParsedDDLFile();
        switch (parsedDDLFile.getTableType()) {
            case HIVE:
                return hiveTableDefinitionFor(tableDefinitionDescriptor);
            case JDBC:
                return jdbcTableDefinitionFor(tableDefinitionDescriptor);
            default:
                throw new IllegalArgumentException("unknown table type: " + parsedDDLFile.getTableType());
        }
    }

    private static HiveTableDefinition hiveTableDefinitionFor(ConventionTableDefinitionDescriptor tableDefinitionDescriptor)
    {
        HiveDataSource dataSource = new FileBasedHiveDataSource(tableDefinitionDescriptor);
        return hiveTableDefinition(
                getTableHandle(tableDefinitionDescriptor),
                tableDefinitionDescriptor.getParsedDDLFile().getContent(),
                dataSource);
    }

    private static TableDefinition jdbcTableDefinitionFor(ConventionTableDefinitionDescriptor tableDefinitionDescriptor)
    {
        RelationalDataSource dataSource = new FileBasedRelationalDataSource(tableDefinitionDescriptor);
        return relationalTableDefinition(
                getTableHandle(tableDefinitionDescriptor),
                tableDefinitionDescriptor.getParsedDDLFile().getContent(),
                dataSource);
    }

    private static TableHandle getTableHandle(ConventionTableDefinitionDescriptor tableDefinitionDescriptor)
    {
        TableHandle tableHandle = tableHandle(tableDefinitionDescriptor.getName());
        Optional<String> schema = tableDefinitionDescriptor.getParsedDDLFile().getSchema();
        if (schema.isPresent()) {
            tableHandle = tableHandle.inSchema(schema.get());
        }
        return tableHandle;
    }

    private static TableDefinitionRepositoryKey asRepositoryKey(TableHandle tableHandle)
    {
        return new TableDefinitionRepositoryKey(tableHandle.getName(), tableHandle.getSchema());
    }

    private static TableDefinitionRepositoryKey asRepositoryKey(String name)
    {
        return new TableDefinitionRepositoryKey(name, Optional.empty());
    }

    private static class TableDefinitionRepositoryKey
    {
        private final String name;
        private final Optional<String> schema;

        public TableDefinitionRepositoryKey(String name, Optional<String> schema)
        {
            this.name = requireNonNull(name, "name is null");
            this.schema = requireNonNull(schema, "schema is null");
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TableDefinitionRepositoryKey that = (TableDefinitionRepositoryKey) o;
            return Objects.equals(name, that.name) &&
                    Objects.equals(schema, that.schema);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, schema);
        }

        @Override
        public String toString()
        {
            if (schema.isPresent()) {
                return schema.get() + "." + name;
            }
            return name;
        }
    }
}
