/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.prestosql.tempto.examples;

import com.google.common.collect.ImmutableList;
import io.prestosql.tempto.fulfillment.table.kafka.KafkaTableManager;
import io.prestosql.tempto.runner.TemptoRunner;
import io.prestosql.tempto.runner.TemptoRunnerCommandLineParser;

import static io.prestosql.tempto.internal.configuration.TestConfigurationFactory.DEFAULT_TEST_CONFIGURATION_LOCATION;

public class TemptoExamples
{
    public static void main(String[] args)
    {
        TemptoRunnerCommandLineParser parser = TemptoRunnerCommandLineParser
                .builder("tempto examples")
                .setTestsPackage("io.prestosql.tempto.examples", false)
                .setConfigFile(DEFAULT_TEST_CONFIGURATION_LOCATION, true)
                .build();
        TemptoRunner.runTempto(
                parser,
                args,
                () -> ImmutableList.of(),
                () -> ImmutableList.of(),
                () -> ImmutableList.of(),
                () -> ImmutableList.of(KafkaTableManager.class),
                () -> ImmutableList.of());
    }
}
