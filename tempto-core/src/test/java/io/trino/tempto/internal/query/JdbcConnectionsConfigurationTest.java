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

import com.google.common.collect.ImmutableList;
import io.trino.tempto.internal.configuration.YamlConfiguration;
import io.trino.tempto.query.JdbcConnectivityParamsState;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class JdbcConnectionsConfigurationTest
{
    private static final YamlConfiguration CONFIGURATION = new YamlConfiguration("""
            databases:
              a:
                jdbc_driver_class: com.acme.ADriver
                jdbc_url: jdbc:a://localhost:8080
                jdbc_user: auser
                jdbc_password: apassword

              b:
                jdbc_driver_class: com.acme.BDriver
                jdbc_url: jdbc:b://localhost:8080
                jdbc_user: buser
                jdbc_password: bpassword
                jdbc_pooling: false
                jdbc_jar: /path/to/jar.jar
                prepare_statement: USE schema
                kerberos_principal: HIVE@EXAMPLE.COM
                kerberos_keytab: example.keytab

              c:
                jdbc_driver_class: com.acme.ADriver
                jdbc_url: jdbc:c://localhost:8080
                jdbc_user: cuser

              b_alias:
                alias: b

              non_jdbc_db:
                blah: true
            """);

    private static final YamlConfiguration BAD_CONFIGURATION = new YamlConfiguration("""
            databases:
              a:
                alias: b

              b:
                alias: c

              c:
                alias: a

              d:
                alias: blah
            """);

    private static final JdbcConnectivityParamsState EXPECTED_A_JDBC_CONNECTIVITY_PARAMS =
            JdbcConnectivityParamsState.builder()
                    .setName("a")
                    .setDriverClass("com.acme.ADriver")
                    .setUrl("jdbc:a://localhost:8080")
                    .setUser("auser")
                    .setPassword("apassword")
                    .build();

    private static final JdbcConnectivityParamsState EXPECTED_B_JDBC_CONNECTIVITY_PARAMS =
            JdbcConnectivityParamsState.builder()
                    .setName("b")
                    .setDriverClass("com.acme.BDriver")
                    .setUrl("jdbc:b://localhost:8080")
                    .setUser("buser")
                    .setPassword("bpassword")
                    .setJar(Optional.of("/path/to/jar.jar"))
                    .setPrepareStatements(ImmutableList.of("USE schema"))
                    .setKerberosPrincipal(Optional.of("HIVE@EXAMPLE.COM"))
                    .setKerberosKeytab(Optional.of("example.keytab"))
                    .build();

    private static final JdbcConnectivityParamsState EXPECTED_C_JDBC_CONNECTIVITY_PARAMS =
            JdbcConnectivityParamsState.builder()
                    .setName("c")
                    .setDriverClass("com.acme.ADriver")
                    .setUrl("jdbc:c://localhost:8080")
                    .setUser("cuser")
                    .setPassword("")
                    .build();

    private static final JdbcConnectivityParamsState EXPECTED_B_ALIAS_JDBC_CONNECTIVITY_PARAMS =
            JdbcConnectivityParamsState.builder()
                    .setName("b_alias")
                    .setDriverClass("com.acme.BDriver")
                    .setUrl("jdbc:b://localhost:8080")
                    .setUser("buser")
                    .setPassword("bpassword")
                    .setJar(Optional.of("/path/to/jar.jar"))
                    .setPrepareStatements(ImmutableList.of("USE schema"))
                    .setKerberosPrincipal(Optional.of("HIVE@EXAMPLE.COM"))
                    .setKerberosKeytab(Optional.of("example.keytab"))
                    .build();

    private final JdbcConnectionsConfiguration jdbcConnectionConfiguration = new JdbcConnectionsConfiguration(CONFIGURATION);
    private final JdbcConnectionsConfiguration jdbcBadConnectionConfiguration = new JdbcConnectionsConfiguration(BAD_CONFIGURATION);

    @Test
    public void listDatabaseConnectionConfigurations()
    {
        assertThat(jdbcConnectionConfiguration.getDefinedJdbcConnectionNames())
                .isEqualTo(Set.of("a", "b", "c", "b_alias"));
    }

    @Test
    public void getConnectionConfiguration()
    {
        JdbcConnectivityParamsState a = jdbcConnectionConfiguration.getConnectionConfiguration("a");
        JdbcConnectivityParamsState b = jdbcConnectionConfiguration.getConnectionConfiguration("b");
        JdbcConnectivityParamsState c = jdbcConnectionConfiguration.getConnectionConfiguration("c");

        assertThat(a).isEqualTo(EXPECTED_A_JDBC_CONNECTIVITY_PARAMS);
        assertThat(b).isEqualTo(EXPECTED_B_JDBC_CONNECTIVITY_PARAMS);
        assertThat(c).isEqualTo(EXPECTED_C_JDBC_CONNECTIVITY_PARAMS);
    }

    @Test
    public void getConnectionConfigurationForAlias()
    {
        JdbcConnectivityParamsState bAlias = jdbcConnectionConfiguration.getConnectionConfiguration("b_alias");

        assertThat(bAlias).isEqualTo(EXPECTED_B_ALIAS_JDBC_CONNECTIVITY_PARAMS);
    }

    @Test
    public void getConnectionConfigurationForUnresolvableAlias()
    {
        assertThatThrownBy(() -> jdbcBadConnectionConfiguration.getConnectionConfiguration("d"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void getConnectionConfigurationForLoopingAlias()
    {
        assertThatThrownBy(() -> jdbcBadConnectionConfiguration.getConnectionConfiguration("a"))
                .isInstanceOf(IllegalStateException.class);
    }
}
