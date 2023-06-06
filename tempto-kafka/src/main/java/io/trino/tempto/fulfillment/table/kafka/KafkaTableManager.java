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
package io.trino.tempto.fulfillment.table.kafka;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.fulfillment.table.MutableTableRequirement;
import io.trino.tempto.fulfillment.table.TableDefinition;
import io.trino.tempto.fulfillment.table.TableHandle;
import io.trino.tempto.fulfillment.table.TableInstance;
import io.trino.tempto.fulfillment.table.TableManager;
import io.trino.tempto.internal.fulfillment.table.TableName;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.google.inject.Singleton;

import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import static com.google.common.primitives.Shorts.checkedCast;
import static java.util.Objects.requireNonNull;

@TableManager.Descriptor(tableDefinitionClass = KafkaTableDefinition.class, type = "KAFKA")
@Singleton
public class KafkaTableManager
        implements TableManager<KafkaTableDefinition>
{
    private final String databaseName;
    private final Configuration brokerConfiguration;

    @Inject
    public KafkaTableManager(
            @Named("databaseName") String databaseName,
            @Named("broker") Configuration brokerConfiguration,
            Injector injector)
    {
        this.databaseName = requireNonNull(databaseName, "databaseName is null");
        this.brokerConfiguration = requireNonNull(brokerConfiguration, "brokerConfiguration is null");
        requireNonNull(injector, "injector is null");
    }

    @Override
    public TableInstance<KafkaTableDefinition> createImmutable(KafkaTableDefinition tableDefinition, TableHandle tableHandle)
    {
        deleteTopic(tableDefinition.getTopic());
        createTopic(tableDefinition.getTopic(), tableDefinition.getPartitionsCount(), tableDefinition.getReplicationLevel());
        insertDataIntoTopic(tableDefinition.getTopic(), tableDefinition.getDataSource());
        TableName createdTableName = new TableName(
                tableHandle.getDatabase().orElse(getDatabaseName()),
                tableHandle.getSchema(),
                tableHandle.getName(),
                tableHandle.getName());
        return new KafkaTableInstance(createdTableName, tableDefinition);
    }

    private void deleteTopic(String topic)
    {
        try (AdminClient kafkaAdminClient = getAdminClient()) {
            ListTopicsResult topics = kafkaAdminClient.listTopics();
            Set<String> names = topics.names().get();

            if (names.contains(topic)) {
                kafkaAdminClient.deleteTopics(ImmutableList.of(topic)).all().get();
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Could not delete topic " + topic, e);
        }
    }

    private void createTopic(String topic, int partitionsCount, int replicationLevel)
    {
        try (AdminClient kafkaAdminClient = getAdminClient()) {
            kafkaAdminClient.createTopics(ImmutableList.of(new NewTopic(topic, partitionsCount, checkedCast(replicationLevel)))).all().get();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void insertDataIntoTopic(String topic, KafkaDataSource dataSource)
    {
        Producer<byte[], byte[]> producer = new KafkaProducer<>(getKafkaProperties());

        Iterator<KafkaMessage> messages = dataSource.getMessages();
        while (messages.hasNext()) {
            KafkaMessage message = messages.next();
            try {
                producer.send(new ProducerRecord<>(
                        topic,
                        message.getPartition().isPresent() ? message.getPartition().getAsInt() : null,
                        message.getKey().orElse(null),
                        message.getValue())).get();
            }
            catch (Exception e) {
                throw new RuntimeException("could not send message to topic " + topic);
            }
        }
    }

    private Properties getKafkaProperties()
    {
        Properties props = new Properties();

        props.put("bootstrap.servers", brokerConfiguration.getStringMandatory("host") + ":" + brokerConfiguration.getIntMandatory("port"));
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        for (String key : brokerConfiguration.listKeys()) {
            if (key.equals("host") || key.equals("port")) {
                continue;
            }
            props.put(key, brokerConfiguration.getStringMandatory(key));
        }
        return props;
    }

    @Override
    public TableInstance<KafkaTableDefinition> createMutable(KafkaTableDefinition tableDefinition, MutableTableRequirement.State state, TableHandle tableHandle)
    {
        throw new IllegalArgumentException("Mutable tables are not supported by KafkaTableManager");
    }

    @Override
    public void dropTable(TableName tableName)
    {
        throw new IllegalArgumentException("dropTable not supported by KafkaTableManager");
    }

    @Override
    public void dropStaleMutableTables()
    {
        // noop
    }

    @Override
    public String getDatabaseName()
    {
        return databaseName;
    }

    @Override
    public Class<? extends TableDefinition> getTableDefinitionClass()
    {
        return KafkaTableDefinition.class;
    }

    private AdminClient getAdminClient()
    {
        return KafkaAdminClient.create(getKafkaProperties());
    }
}
