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
package io.trino.tempto.fulfillment.table;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.tempto.TemptoPlugin;
import io.trino.tempto.fulfillment.table.hive.HiveDataSource;
import io.trino.tempto.fulfillment.table.hive.HiveTableDefinition;
import io.trino.tempto.fulfillment.table.jdbc.RelationalDataSource;
import io.trino.tempto.internal.convention.tabledefinitions.ConventionTableDefinitionDescriptor;
import io.trino.tempto.internal.convention.tabledefinitions.FileBasedHiveDataSource;
import io.trino.tempto.internal.convention.tabledefinitions.FileBasedRelationalDataSource;
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
import java.util.ServiceLoader;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.tempto.fulfillment.table.TableHandle.tableHandle;
import static io.trino.tempto.fulfillment.table.hive.HiveTableDefinition.hiveTableDefinition;
import static io.trino.tempto.fulfillment.table.jdbc.RelationalTableDefinition.relationalTableDefinition;
import static io.trino.tempto.internal.convention.ConventionTestsUtils.getConventionsTestsPath;
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

    private static TableDefinitionsRepository TABLE_DEFINITIONS_REPOSITORY;

    public static TableDefinition tableDefinition(TableHandle tableHandle)
    {
        return tableDefinitionsRepository().get(tableHandle);
    }

    public static TableDefinitionsRepository tableDefinitionsRepository()
    {
        if (TABLE_DEFINITIONS_REPOSITORY == null) {
            List<TemptoPlugin> plugins = ImmutableList.copyOf(ServiceLoader.load(TemptoPlugin.class).iterator());
            List<TableDefinition> tables = Stream.concat(
                    plugins.stream()
                    .flatMap(plugin -> plugin.getTables().stream()),
                    getAllConventionBasedTableDefinitions().stream())
                    .collect(toImmutableList());
            TABLE_DEFINITIONS_REPOSITORY = new TableDefinitionsRepository(tables);
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
