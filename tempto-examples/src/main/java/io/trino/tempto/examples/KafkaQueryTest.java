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
package io.trino.tempto.examples;

import com.google.common.collect.ImmutableList;
import io.trino.tempto.ProductTest;
import io.trino.tempto.Requirement;
import io.trino.tempto.RequirementsProvider;
import io.trino.tempto.Requires;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.fulfillment.table.kafka.KafkaMessage;
import io.trino.tempto.fulfillment.table.kafka.KafkaTableDefinition;
import io.trino.tempto.fulfillment.table.kafka.ListKafkaDataSource;
import io.trino.tempto.query.QueryResult;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.fulfillment.table.TableRequirements.immutableTable;
import static io.trino.tempto.fulfillment.table.kafka.KafkaMessageContentsBuilder.contentsBuilder;
import static io.trino.tempto.query.QueryExecutor.query;
import static java.lang.String.format;

public class KafkaQueryTest
        extends ProductTest
{
    private static final String KAFKA_CATALOG = "kafka";
    private static final String SIMPLE_KEY_AND_VALUE_TABLE_NAME = "default.simple_key_and_value";
    private static final String SIMPLE_KEY_AND_VALUE_TOPIC_NAME = "simple_key_and_value";

    // kafka-connectors requires tables to be predefined in presto configuration
    // the requirements here will be used to verify that table actually exists and to
    // create topics and propagate them with data

    private static class SimpleKeyAndValueTable
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return immutableTable(new KafkaTableDefinition(
                    SIMPLE_KEY_AND_VALUE_TABLE_NAME,
                    SIMPLE_KEY_AND_VALUE_TOPIC_NAME,
                    new ListKafkaDataSource(ImmutableList.of(
                            new KafkaMessage(
                                    contentsBuilder().appendUTF8("jasio,1").build(),
                                    contentsBuilder().appendUTF8("ania,2").build()),
                            new KafkaMessage(
                                    contentsBuilder().appendUTF8("piotr,3").build(),
                                    contentsBuilder().appendUTF8("kasia,4").build()))),
                    1,
                    1));
        }
    }

    @Test(groups = "kafka_query")
    @Requires(SimpleKeyAndValueTable.class)
    public void testSelectSimpleKeyAndValue()
    {
        QueryResult queryResult = query(format("select varchar_key, bigint_key, varchar_value, bigint_value from %s.%s", KAFKA_CATALOG, SIMPLE_KEY_AND_VALUE_TABLE_NAME));
        assertThat(queryResult).containsOnly(
                row("jasio", 1, "ania", 2),
                row("piotr", 3, "kasia", 4));
    }

}
