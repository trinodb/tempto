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

import com.google.common.collect.MapMaker;
import io.prestosql.tempto.fulfillment.table.hive.HiveDataSource;
import io.prestosql.tempto.fulfillment.table.hive.HiveTableDefinition;
import io.prestosql.tempto.fulfillment.table.jdbc.RelationalDataSource;
import io.prestosql.tempto.internal.ReflectionHelper;
import io.prestosql.tempto.internal.convention.tabledefinitions.ConventionTableDefinitionDescriptor;
import io.prestosql.tempto.internal.convention.tabledefinitions.FileBasedHiveDataSource;
import io.prestosql.tempto.internal.convention.tabledefinitions.FileBasedRelationalDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.tempto.fulfillment.table.TableHandle.tableHandle;
import static io.prestosql.tempto.fulfillment.table.hive.HiveTableDefinition.hiveTableDefinition;
import static io.prestosql.tempto.fulfillment.table.jdbc.RelationalTableDefinition.relationalTableDefinition;
import static io.prestosql.tempto.internal.ReflectionHelper.getFieldsAnnotatedWith;
import static io.prestosql.tempto.internal.convention.ConventionTestsUtils.getConventionsTestsPath;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.newDirectoryStream;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Stores {@link TableDefinition} mapped by names.
 */
public class TableDefinitionsRepository
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TableDefinitionsRepository.class);

    private static final String DATASETS_PATH_PART = "datasets";

    /**
     * An annotation for {@link TableDefinition} static fields
     * that should be registered in {@link TableDefinitionsRepository}.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface RepositoryTableDefinition
    {
    }

    private static final List<TableDefinition> SCANNED_TABLE_DEFINITIONS;

    private static final TableDefinitionsRepository TABLE_DEFINITIONS_REPOSITORY;

    static {
        // ExceptionInInitializerError is not always appropriately logged, lets log exceptions explicitly here
        try {
            SCANNED_TABLE_DEFINITIONS =
                    getFieldsAnnotatedWith(RepositoryTableDefinition.class)
                            .stream()
                            .map(ReflectionHelper::<TableDefinition>getStaticFieldValue)
                            .collect(toList());

            TABLE_DEFINITIONS_REPOSITORY = new TableDefinitionsRepository(SCANNED_TABLE_DEFINITIONS);
            // TODO: since TestNG has no listener that can be run before tests factory, this has to be initialized here
            TABLE_DEFINITIONS_REPOSITORY.getAllConventionBasedTableDefinitions().stream()
                    .forEach(TABLE_DEFINITIONS_REPOSITORY::register);
        }
        catch (RuntimeException e) {
            LOGGER.error("Error during TableDefinitionsRepository initialization", e);
            throw e;
        }
    }

    public static TableDefinition tableDefinition(TableHandle tableHandle)
    {
        return tableDefinitionsRepository().get(tableHandle);
    }

    public static TableDefinitionsRepository tableDefinitionsRepository()
    {
        return TABLE_DEFINITIONS_REPOSITORY;
    }

    private final Map<TableDefinitionRepositoryKey, TableDefinition> tableDefinitions = new MapMaker().makeMap();

    public TableDefinitionsRepository()
    {
    }

    public TableDefinitionsRepository(Collection<TableDefinition> tableDefinitions)
    {
        tableDefinitions.stream().forEach(this::register);
    }

    public <T extends TableDefinition> T register(T tableDefinition)
    {
        TableDefinitionRepositoryKey repositoryKey = asRepositoryKey(tableDefinition.getTableHandle());
        checkState(!tableDefinitions.containsKey(repositoryKey), "duplicated table definition: %s", repositoryKey);
        tableDefinitions.put(repositoryKey, tableDefinition);
        return tableDefinition;
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
        else {
            return getAllConventionTableDefinitionDescriptors(dataSetsPath.get())
                    .stream()
                    .map(TableDefinitionsRepository::tableDefinitionFor)
                    .collect(toList());
        }
    }

    private static List<ConventionTableDefinitionDescriptor> getAllConventionTableDefinitionDescriptors(Path dataSetsPath)
    {
        if (exists(dataSetsPath)) {
            LOGGER.debug("Data sets configuration for path: {}", dataSetsPath);

            try {
                return StreamSupport.stream(newDirectoryStream(dataSetsPath, "*.ddl").spliterator(), false)
                        .map(ddlFile -> new ConventionTableDefinitionDescriptor(ddlFile))
                        .collect(toList());
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
