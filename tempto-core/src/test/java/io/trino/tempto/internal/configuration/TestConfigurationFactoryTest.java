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

package io.trino.tempto.internal.configuration;

import io.trino.tempto.configuration.Configuration;
import org.junit.jupiter.api.Test;

import static io.trino.tempto.internal.configuration.TestConfigurationFactory.TEST_CONFIGURATION_URIS_KEY;
import static org.assertj.core.api.Assertions.assertThat;

public class TestConfigurationFactoryTest
{
    @Test
    public void readTwoTestConfigurations()
    {
        System.setProperty(TEST_CONFIGURATION_URIS_KEY, "/configuration/global-configuration-tempto.yaml,/configuration/local-configuration-tempto.yaml");

        Configuration configuration = TestConfigurationFactory.createTestConfiguration();

        assertThat(configuration.getStringMandatory("value.local")).isEqualTo("local");
        assertThat(configuration.getStringMandatory("value.both")).isEqualTo("local");
        assertThat(configuration.getStringMandatory("value.global")).isEqualTo("global");
        assertThat(configuration.getStringMandatory("value.default")).isEqualTo("default_value");
        assertThat(configuration.getStringMandatory("value.with.dot")).isEqualTo("1");
        assertThat(configuration.getSubconfiguration("value").getIntMandatory("with.dot")).isEqualTo(1);
        assertThat(configuration.getSubconfiguration("value.with").listKeys()).isEmpty();

        assertThat(configuration.getStringMandatory("resolve.local")).isEqualTo("local");
        assertThat(configuration.getStringMandatory("resolve.both")).isEqualTo("local");
        assertThat(configuration.getStringMandatory("resolve.global")).isEqualTo("global");
    }
}
