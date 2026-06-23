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

package io.trino.tempto.internal.fulfillment.hive;

import io.trino.tempto.fulfillment.table.hive.HiveDataSource;
import io.trino.tempto.fulfillment.table.hive.HiveTableDefinition;
import io.trino.tempto.internal.fulfillment.table.TableNameGenerator;
import io.trino.tempto.internal.fulfillment.table.hive.HiveTableInstance;
import io.trino.tempto.internal.fulfillment.table.hive.HiveTableManager;
import io.trino.tempto.internal.fulfillment.table.hive.HiveThriftClient;
import io.trino.tempto.internal.hadoop.hdfs.HdfsDataSourceWriter;
import io.trino.tempto.query.QueryExecutor;
import io.trino.tempto.query.QueryResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.JDBCType;

import static io.trino.tempto.fulfillment.table.MutableTableRequirement.State.CREATED;
import static io.trino.tempto.fulfillment.table.MutableTableRequirement.State.LOADED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HiveTableManagerTest
{
    private static final String ROOT_PATH = "/tests-path";
    private static final String MUTABLE_TABLES_PATH = "/user/hive/warehouse/";

    private static final String NATION_DDL_TEMPLATE =
            """

                CREATE TABLE %NAME%(
                        n_nationid BIGINT,
                        n_name STRING)
                        ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
            """;

    private static final String PARTITIONED_NATION_DDL_TEMPLATE =
            """

                CREATE TABLE %NAME%(
                        n_nationid BIGINT,
                        n_name STRING)
                        ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
            """;

    private QueryExecutor queryExecutor;
    private HdfsDataSourceWriter dataSourceWriter;
    private TableNameGenerator tableNameGenerator;
    private HiveThriftClient hiveThriftClient;
    private HiveTableManager tableManager;

    @BeforeEach
    public void setup()
    {
        queryExecutor = mock(QueryExecutor.class);
        dataSourceWriter = mock(HdfsDataSourceWriter.class);
        tableNameGenerator = mock(TableNameGenerator.class);
        hiveThriftClient = mock(HiveThriftClient.class);

        when(tableNameGenerator.generateMutableTableNameInDatabase(any())).thenReturn("nation_randomSuffix");
        tableManager = new HiveTableManager(queryExecutor, dataSourceWriter, tableNameGenerator, hiveThriftClient, ROOT_PATH, "database", false, false);
    }

    @Test
    public void shouldCreateHiveImmutableTable()
    {
        String expectedTableLocation = "/tests-path/some/table/in/hdfs";
        String expectedTableName = "nation";
        String expectedTableNameInDatabase = "nation";

        HiveTableDefinition nationDefinition = getNationHiveTableDefinition();
        HiveTableInstance nationTableInstance = (HiveTableInstance) tableManager.createImmutable(nationDefinition);

        assertThat(nationTableInstance.getName()).isEqualTo(expectedTableName);
        assertThat(nationTableInstance.getNameInDatabase()).isEqualTo(expectedTableNameInDatabase);

        verify(dataSourceWriter).ensureDataOnHdfs(eq(expectedTableLocation), any());
        verify(queryExecutor).executeQuery(eq(expandDDLTemplate(NATION_DDL_TEMPLATE, expectedTableNameInDatabase, expectedTableLocation)));
    }

    @Test
    public void shouldCreateHiveMutableTableLoadedNotPartitioned()
    {
        String expectedTableLocation = "/user/hive/warehouse/nation_randomSuffix";
        String expectedTableName = "nation";
        String expectedTableNameInDatabase = "nation_randomSuffix";
        when(queryExecutor.executeQuery("SHOW CREATE TABLE nation_randomSuffix"))
                .thenReturn(QueryResult.forSingleValue(JDBCType.VARCHAR, "CRATE TABLE NATION(....) LOCATION '/user/hive/warehouse/nation_randomSuffix'"));

        HiveTableDefinition tableDefinition = getNationHiveTableDefinition();
        HiveTableInstance tableInstance = (HiveTableInstance) tableManager.createMutable(tableDefinition, LOADED);

        assertThat(tableInstance.getNameInDatabase()).isEqualTo(expectedTableNameInDatabase);
        assertThat(tableInstance.getName()).isEqualTo(expectedTableName);
        verify(dataSourceWriter).ensureDataOnHdfs(eq(expectedTableLocation), any());
        verify(queryExecutor).executeQuery(eq(expandDDLTemplate(NATION_DDL_TEMPLATE, expectedTableNameInDatabase)));
    }

    @Test
    public void shouldCreateHiveMutableTableCreatedNotPartitioned()
    {
        String expectedTableName = "nation";
        String expectedTableNameInDatabase = "nation_randomSuffix";

        HiveTableDefinition tableDefinition = getNationHiveTableDefinition();
        HiveTableInstance tableInstance = (HiveTableInstance) tableManager.createMutable(tableDefinition, CREATED);

        assertThat(tableInstance.getNameInDatabase()).isEqualTo(expectedTableNameInDatabase);
        assertThat(tableInstance.getName()).isEqualTo(expectedTableName);
        verify(queryExecutor).executeQuery(eq(expandDDLTemplate(NATION_DDL_TEMPLATE, expectedTableNameInDatabase)));
    }

    @Test
    public void shouldCreateHiveMutableTableLoadedPartitioned()
    {
        String expectedTableName = "nation";
        String expectedTableNameInDatabase = "nation_randomSuffix";
        String expectedTableLocation = MUTABLE_TABLES_PATH + expectedTableNameInDatabase;
        String expectedPartition0Location = expectedTableLocation + "/partition_0";
        String expectedPartition1Location = expectedTableLocation + "/partition_1";
        when(queryExecutor.executeQuery("SHOW CREATE TABLE nation_randomSuffix"))
                .thenReturn(QueryResult.forSingleValue(JDBCType.VARCHAR, "CRATE TABLE NATION(....) LOCATION '/user/hive/warehouse/nation_randomSuffix'"));

        HiveTableDefinition tableDefinition = getPartitionedNationHiveTableDefinition();
        HiveTableInstance tableInstance = (HiveTableInstance) tableManager.createMutable(tableDefinition, LOADED);

        assertThat(tableInstance.getNameInDatabase()).isEqualTo(expectedTableNameInDatabase);
        assertThat(tableInstance.getName()).isEqualTo(expectedTableName);
        verify(dataSourceWriter).ensureDataOnHdfs(eq(expectedPartition0Location), any());
        verify(dataSourceWriter).ensureDataOnHdfs(eq(expectedPartition1Location), any());
        verify(queryExecutor).executeQuery(eq(expandDDLTemplate(PARTITIONED_NATION_DDL_TEMPLATE, expectedTableNameInDatabase)));
        verify(queryExecutor).executeQuery(eq("ALTER TABLE " + expectedTableNameInDatabase + " ADD PARTITION (pc=0) LOCATION '" + expectedPartition0Location + "'"));
        verify(queryExecutor).executeQuery(eq("ALTER TABLE " + expectedTableNameInDatabase + " ADD PARTITION (pc=1) LOCATION '" + expectedPartition1Location + "'"));
    }

    @Test
    public void shouldCreateHiveMutableTableCreatedPartitioned()
    {
        String expectedTableName = "nation";
        String expectedTableNameInDatabase = "nation_randomSuffix";
        String expectedTableLocation = MUTABLE_TABLES_PATH + expectedTableNameInDatabase;
        String expectedPartition0Location = expectedTableLocation + "/partition_0";
        String expectedPartition1Location = expectedTableLocation + "/partition_1";
        when(queryExecutor.executeQuery("SHOW CREATE TABLE nation_randomSuffix"))
                .thenReturn(QueryResult.forSingleValue(JDBCType.VARCHAR, "CRATE TABLE NATION(....) LOCATION '/user/hive/warehouse/nation_randomSuffix'"));

        HiveTableDefinition tableDefinition = getPartitionedNationHiveTableDefinition();
        HiveTableInstance tableInstance = (HiveTableInstance) tableManager.createMutable(tableDefinition, CREATED);

        assertThat(tableInstance.getNameInDatabase()).isEqualTo(expectedTableNameInDatabase);
        assertThat(tableInstance.getName()).isEqualTo(expectedTableName);
        verify(queryExecutor).executeQuery(eq(expandDDLTemplate(PARTITIONED_NATION_DDL_TEMPLATE, expectedTableNameInDatabase)));
        verify(queryExecutor).executeQuery(eq("ALTER TABLE " + expectedTableNameInDatabase + " ADD PARTITION (pc=0) LOCATION '" + expectedPartition0Location + "'"));
        verify(queryExecutor).executeQuery(eq("ALTER TABLE " + expectedTableNameInDatabase + " ADD PARTITION (pc=1) LOCATION '" + expectedPartition1Location + "'"));
    }

    private String expandDDLTemplate(String template, String tableName)
    {
        return expandDDLTemplate(template, tableName, null);
    }

    private String expandDDLTemplate(String template, String tableName, String location)
    {
        String ddl = template.replace("%NAME%", tableName);
        if (location != null) {
            ddl += " LOCATION '" + location + "'";
        }
        return ddl;
    }

    private HiveTableDefinition getNationHiveTableDefinition()
    {
        HiveDataSource nationDataSource = mockDataSource("some/table/in/hdfs");
        return HiveTableDefinition.builder("nation")
                .setDataSource(nationDataSource)
                .setCreateTableDDLTemplate(NATION_DDL_TEMPLATE)
                .build();
    }

    private HiveTableDefinition getPartitionedNationHiveTableDefinition()
    {
        return HiveTableDefinition.builder("nation")
                .setCreateTableDDLTemplate(PARTITIONED_NATION_DDL_TEMPLATE)
                .addPartition("pc=0", mockDataSource("not/important"))
                .addPartition("pc=1", mockDataSource("not/important"))
                .build();
    }

    private HiveDataSource mockDataSource(String pathSuffix)
    {
        HiveDataSource dataSource = mock(HiveDataSource.class);
        when(dataSource.getPathSuffix()).thenReturn(pathSuffix);
        return dataSource;
    }
}
