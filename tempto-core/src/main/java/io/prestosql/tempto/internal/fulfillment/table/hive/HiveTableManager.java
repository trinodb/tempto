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
package io.prestosql.tempto.internal.fulfillment.table.hive;

import com.google.inject.Inject;
import io.prestosql.tempto.fulfillment.table.MutableTableRequirement.State;
import io.prestosql.tempto.fulfillment.table.TableDefinition;
import io.prestosql.tempto.fulfillment.table.TableHandle;
import io.prestosql.tempto.fulfillment.table.TableManager;
import io.prestosql.tempto.fulfillment.table.hive.HiveDataSource;
import io.prestosql.tempto.fulfillment.table.hive.HiveTableDefinition;
import io.prestosql.tempto.internal.fulfillment.table.AbstractTableManager;
import io.prestosql.tempto.internal.fulfillment.table.TableName;
import io.prestosql.tempto.internal.fulfillment.table.TableNameGenerator;
import io.prestosql.tempto.internal.hadoop.hdfs.HdfsDataSourceWriter;
import io.prestosql.tempto.query.QueryExecutor;
import io.prestosql.tempto.query.QueryResult;
import org.slf4j.Logger;

import javax.inject.Named;
import javax.inject.Singleton;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.tempto.fulfillment.table.MutableTableRequirement.State.LOADED;
import static io.prestosql.tempto.fulfillment.table.MutableTableRequirement.State.PREPARED;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static org.slf4j.LoggerFactory.getLogger;

@TableManager.Descriptor(tableDefinitionClass = HiveTableDefinition.class, type = "HIVE")
@Singleton
public class HiveTableManager
        extends AbstractTableManager<HiveTableDefinition>
{
    private static final Logger LOGGER = getLogger(HiveTableManager.class);

    private final QueryExecutor queryExecutor;
    private final HdfsDataSourceWriter hdfsDataSourceWriter;
    private final String testDataBasePath;
    private final HiveThriftClient hiveThriftClient;
    private final String databaseName;
    private final boolean injectStatsForImmutableTables;
    private final boolean injectStatsForMutableTables;

    @Inject
    public HiveTableManager(
            QueryExecutor queryExecutor,
            HdfsDataSourceWriter hdfsDataSourceWriter,
            TableNameGenerator tableNameGenerator,
            @Named("tests.hdfs.path") String testDataBasePath,
            @Named("databaseName") String databaseName,
            @Named("inject_stats_for_immutable_tables") boolean injectStatsForImmutableTables,
            @Named("inject_stats_for_mutable_tables") boolean injectStatsForMutableTables,
            @Named("metastore.host") String thriftHost,
            @Named("metastore.port") String thriftPort)
    {
        this(
                queryExecutor,
                hdfsDataSourceWriter,
                tableNameGenerator,
                new HiveThriftClient(thriftHost, parseInt(thriftPort)),
                testDataBasePath,
                databaseName,
                injectStatsForImmutableTables,
                injectStatsForMutableTables);
    }

    public HiveTableManager(
            QueryExecutor queryExecutor,
            HdfsDataSourceWriter hdfsDataSourceWriter,
            TableNameGenerator tableNameGenerator,
            HiveThriftClient hiveThriftClient,
            String testDataBasePath,
            String databaseName,
            boolean injectStatsForImmutableTables,
            boolean injectStatsForMutableTables)
    {
        super(queryExecutor, tableNameGenerator);
        this.hiveThriftClient = hiveThriftClient;
        this.databaseName = databaseName;
        this.queryExecutor = checkNotNull(queryExecutor, "queryExecutor is null");
        this.hdfsDataSourceWriter = checkNotNull(hdfsDataSourceWriter, "hdfsDataSourceWriter is null");
        this.testDataBasePath = checkNotNull(testDataBasePath, "testDataBasePath is null");
        this.injectStatsForImmutableTables = injectStatsForImmutableTables;
        this.injectStatsForMutableTables = injectStatsForMutableTables;
    }

    @Override
    public HiveTableInstance createImmutable(HiveTableDefinition tableDefinition, TableHandle tableHandle)
    {
        try {
            return doCreateImmutable(tableDefinition, tableHandle);
        }
        catch (RuntimeException e) {
            throw new RuntimeException("Failed to create table " + tableHandle, e);
        }
    }

    private HiveTableInstance doCreateImmutable(HiveTableDefinition tableDefinition, TableHandle tableHandle)
    {
        checkState(!tableDefinition.isPartitioned(), "Partitioning not supported for immutable tables");
        TableName tableName = createImmutableTableName(tableHandle);
        LOGGER.debug("creating immutable table {}", tableHandle.getName());

        String tableDataPath = getImmutableTableHdfsPath(tableDefinition.getDataSource());
        uploadTableData(tableDataPath, tableDefinition.getDataSource());

        dropTableIgnoreError(tableName);
        createTable(tableDefinition, tableName, Optional.of(tableDataPath));
        markTableAsExternal(tableName);
        if (tableDefinition.getInjectStats().orElse(injectStatsForImmutableTables)) {
            injectStatistics(tableDefinition, tableName, tableDefinition.getInjectStats().orElse(false));
        }

        return new HiveTableInstance(tableName, tableDefinition);
    }

    @Override
    public HiveTableInstance createMutable(HiveTableDefinition tableDefinition, State state, TableHandle tableHandle)
    {
        try {
            return doCreateMutable(tableDefinition, state, tableHandle);
        }
        catch (RuntimeException e) {
            throw new RuntimeException("Failed to create table " + tableHandle, e);
        }
    }

    private HiveTableInstance doCreateMutable(HiveTableDefinition tableDefinition, State state, TableHandle tableHandle)
    {
        TableName tableName = createMutableTableName(tableHandle);
        LOGGER.debug("creating mutable table {}", tableName);

        if (state == PREPARED) {
            return new HiveTableInstance(tableName, tableDefinition);
        }

        createTable(tableDefinition, tableName, Optional.empty());

        if (tableDefinition.isPartitioned()) {
            int partitionId = 0;
            for (HiveTableDefinition.PartitionDefinition partitionDefinition : tableDefinition.getPartitionDefinitions()) {
                String partitionDataPath = getMutableTableHdfsPath(tableName, Optional.of(partitionId));
                if (state == LOADED) {
                    uploadTableData(partitionDataPath, partitionDefinition.getDataSource());
                }
                queryExecutor.executeQuery(partitionDefinition.getAddPartitionTableDDL(tableName, partitionDataPath));
                partitionId++;
            }
        }
        else if (state == LOADED) {
            String tableDataPath = getMutableTableHdfsPath(tableName, Optional.empty());
            uploadTableData(tableDataPath, tableDefinition.getDataSource());
        }

        if (state == LOADED && tableDefinition.getInjectStats().orElse(injectStatsForMutableTables)) {
            injectStatistics(tableDefinition, tableName, tableDefinition.getInjectStats().orElse(false));
        }

        return new HiveTableInstance(tableName, tableDefinition);
    }

    @Override
    public String getDatabaseName()
    {
        return databaseName;
    }

    @Override
    public Class<? extends TableDefinition> getTableDefinitionClass()
    {
        return HiveTableDefinition.class;
    }

    private void uploadTableData(String tableDataPath, HiveDataSource dataSource)
    {
        hdfsDataSourceWriter.ensureDataOnHdfs(tableDataPath, dataSource);
    }

    private String getImmutableTableHdfsPath(HiveDataSource dataSource)
    {
        return testDataBasePath + "/" + dataSource.getPathSuffix();
    }

    private String getMutableTableHdfsPath(TableName tableName, Optional<Integer> partitionId)
    {
        QueryResult queryResult = queryExecutor.executeQuery("SHOW CREATE TABLE " + tableName.getNameInDatabase());

        // result spans multiple rows
        StringBuilder value = new StringBuilder();
        for (List<?> row : queryResult.rows()) {
            value.append(getOnlyElement(row));
        }

        Pattern locationPattern = Pattern.compile("LOCATION\\s+'([^']+)'");
        Matcher matcher = locationPattern.matcher(value);
        if (!matcher.find()) {
            throw new IllegalArgumentException("Cant get table location from result of SHOW CREATE TABLE: " + value);
        }
        String location = matcher.group(1);
        verify(!matcher.find(), "Expected only single match of LOCATION in result of SHOW CREATE TABLE");
        if (location.startsWith("hdfs://")) {
            try {
                URI uri = new URI(location);
                location = uri.getPath();
            }
            catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }

        if (partitionId.isPresent()) {
            location += "/partition_" + partitionId.get();
        }
        return location;
    }

    private void createTable(HiveTableDefinition tableDefinition, TableName tableName, Optional<String> tableDataPath)
    {
        tableName.getSchema().ifPresent(schema ->
                queryExecutor.executeQuery("CREATE SCHEMA IF NOT EXISTS " + schema)
        );
        queryExecutor.executeQuery(tableDefinition.getCreateTableDDL(tableName.getNameInDatabase(), tableDataPath));
    }

    private void markTableAsExternal(TableName tableName)
    {
        queryExecutor.executeQuery(format("ALTER TABLE %s SET TBLPROPERTIES('EXTERNAL'='TRUE')", tableName.getNameInDatabase()));
    }

    private void injectStatistics(HiveTableDefinition tableDefinition, TableName tableName, boolean mustInject)
    {
        if (tableDefinition.isPartitioned() || !tableDefinition.getDataSource().getStatistics().isPresent()) {
            checkArgument(!mustInject, "Injecting statistics requested, but injecting is not possible");
            return;
        }
        hiveThriftClient.setStatistics(tableName, tableDefinition.getDataSource().getStatistics().get());
    }

    @Override
    public void close()
    {
        hiveThriftClient.close();
        super.close();
    }
}
