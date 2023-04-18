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
package io.trino.tempto.internal.query;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.query.QueryExecutionException;
import io.trino.tempto.query.QueryResult;

import java.net.InetSocketAddress;
import java.sql.JDBCType;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.StreamSupport;

import static com.google.common.collect.Lists.newArrayList;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class CassandraQueryExecutor
        implements AutoCloseable
{
    private static final Map<DataType, JDBCType> typeMapping = ImmutableMap.<DataType, JDBCType>builder()
            .put(DataTypes.ASCII, JDBCType.VARCHAR)
            .put(DataTypes.BIGINT, JDBCType.BIGINT)
            .put(DataTypes.BLOB, JDBCType.BLOB)
            .put(DataTypes.BOOLEAN, JDBCType.BOOLEAN)
            .put(DataTypes.COUNTER, JDBCType.BIGINT)
            .put(DataTypes.DATE, JDBCType.DATE)
            .put(DataTypes.DECIMAL, JDBCType.DECIMAL)
            .put(DataTypes.DOUBLE, JDBCType.DOUBLE)
            .put(DataTypes.FLOAT, JDBCType.REAL)
            .put(DataTypes.INT, JDBCType.INTEGER)
            .put(DataTypes.SMALLINT, JDBCType.SMALLINT)
            .put(DataTypes.TIME, JDBCType.TIME)
            .put(DataTypes.TIMESTAMP, JDBCType.TIMESTAMP)
            .put(DataTypes.TINYINT, JDBCType.TINYINT)
            .put(DataTypes.TEXT, JDBCType.VARCHAR)
            .build();
    private final CqlSession session;


    public static class TypeNotSupportedException
            extends IllegalStateException
    {
        TypeNotSupportedException(DataType type)
        {
            super(format("Type is not supported: %s.", type));
        }
    }

    public CassandraQueryExecutor(Configuration configuration)
    {
        ProgrammaticDriverConfigLoaderBuilder loader = DriverConfigLoader.programmaticBuilder();
        configuration.getInt("databases.cassandra.basic.request.timeout_seconds")
                .ifPresent(timeout -> loader.withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(timeout)));
        CqlSessionBuilder sessionBuilder = CqlSession.builder()
                .withConfigLoader(loader.build())
                .addContactPoint(new InetSocketAddress(configuration.getStringMandatory("databases.cassandra.host"), configuration.getIntMandatory("databases.cassandra.port")));
        configuration.getString("databases.cassandra.local_datacenter").ifPresent(sessionBuilder::withLocalDatacenter);
        session = sessionBuilder.build();
    }

    public QueryResult executeQuery(String sql)
            throws QueryExecutionException
    {
        ResultSet rs = session.execute(sql);
        ColumnDefinitions definitions = rs.getColumnDefinitions();
        List<JDBCType> types = StreamSupport.stream(definitions.spliterator(), false)
                .map(definition -> getJDBCType(definition.getType()))
                .collect(toList());

        List<String> columnNames = StreamSupport.stream(definitions.spliterator(), false)
                .map(columnDefinition -> columnDefinition.getName().asInternal())
                .collect(toList());

        QueryResult.QueryResultBuilder resultBuilder = new QueryResult.QueryResultBuilder(types, columnNames);

        for (Row row : rs) {
            List<Object> builderRow = newArrayList();
            for (int i = 0; i < types.size(); ++i) {
                builderRow.add(row.getObject(i));
            }
            resultBuilder.addRow(builderRow);
        }

        return resultBuilder.build();
    }

    public CqlSession getSession()
    {
        return session;
    }

    public List<String> getColumnNames(String keySpace, String tableName)
    {
        KeyspaceMetadata keyspaceMetadata = session.getMetadata().getKeyspace(keySpace)
                .orElseThrow(() -> new IllegalArgumentException(format("keyspace %s does not exist", keySpace)));
        TableMetadata tableMetadata = keyspaceMetadata.getTable(tableName)
                .orElseThrow(() -> new IllegalArgumentException(format("table %s.%s does not exist", keySpace, tableName)));
        return tableMetadata.getColumns().keySet().stream().map(CqlIdentifier::asInternal).collect(toList());
    }

    public boolean tableExists(String keySpace, String tableName)
    {
        Optional<KeyspaceMetadata> keyspaceMetadata = session.getMetadata().getKeyspace(keySpace);
        return keyspaceMetadata.map(metadata -> metadata.getTable(tableName).isPresent()).orElse(false);
    }

    public List<String> getTableNames(String keySpace)
    {
        Metadata clusterMetadata = session.getMetadata();
        Optional<KeyspaceMetadata> keyspaceMetadata = clusterMetadata.getKeyspace(keySpace);
        return keyspaceMetadata.map(metadata -> metadata.getTables().keySet().stream()
                .map(CqlIdentifier::asInternal)
                .collect(toList())).orElseGet(ImmutableList::of);
    }

    @Override
    public void close()
    {
        session.close();
    }

    private static JDBCType getJDBCType(DataType type)
    {
        JDBCType jdbcType = typeMapping.get(type);
        if (type == null) {
            throw new TypeNotSupportedException(type);
        }

        return jdbcType;
    }
}
