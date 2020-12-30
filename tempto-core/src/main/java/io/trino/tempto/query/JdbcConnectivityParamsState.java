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
package io.trino.tempto.query;

import com.google.common.collect.ImmutableList;
import io.trino.tempto.context.State;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static java.util.Optional.empty;
import static org.apache.commons.lang3.builder.EqualsBuilder.reflectionEquals;
import static org.apache.commons.lang3.builder.HashCodeBuilder.reflectionHashCode;
import static org.apache.commons.lang3.builder.ToStringBuilder.reflectionToString;

/**
 * Contains configuration required to connect to a JDBC server.
 */
public class JdbcConnectivityParamsState
        implements State
{
    private final String name;
    public final String driverClass;
    public final String url;
    public final String user;
    public final String password;
    public final boolean pooling;
    public final Optional<String> jar;
    public final List<String> prepareStatements;
    public final Optional<String> kerberosPrincipal;
    public final Optional<String> kerberosKeytab;

    private JdbcConnectivityParamsState(
            String name,
            String driverClass,
            String url,
            String user,
            String password,
            boolean pooling,
            Optional<String> jar,
            List<String> prepareStatements,
            Optional<String> kerberosPrincipal,
            Optional<String> kerberosKeytab)
    {
        this.name = requireNonNull(name, "name is null");
        this.driverClass = requireNonNull(driverClass, "driverClass is null");
        this.url = requireNonNull(url, "url is null");
        this.user = requireNonNull(user, "user is null");
        this.password = requireNonNull(password, "password is null");
        this.pooling = pooling;
        this.jar = requireNonNull(jar, "jar is null");
        this.prepareStatements = ImmutableList.copyOf(requireNonNull(prepareStatements, "prepareStatements is null"));
        this.kerberosPrincipal = requireNonNull(kerberosPrincipal, "kerberosPrincipal is null");
        this.kerberosKeytab = requireNonNull(kerberosKeytab, "kerberosKeytab is null");
    }

    @Override
    public Optional<String> getName()
    {
        return Optional.of(name);
    }

    @Override
    public boolean equals(Object o)
    {
        return reflectionEquals(this, o);
    }

    @Override
    public int hashCode()
    {
        return reflectionHashCode(this);
    }

    @Override
    public String toString()
    {
        return reflectionToString(this);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private String name;
        private String driverClass;
        private String url;
        private String user = "";
        private String password = "";
        private boolean pooling = false;
        private Optional<String> jar = empty();
        private List<String> prepareStatements = ImmutableList.of();
        private Optional<String> kerberosPrincipal = empty();
        private Optional<String> kerberosKeytab = empty();

        private Builder() {}

        public Builder setName(String name)
        {
            this.name = name;
            return this;
        }

        public Builder setDriverClass(String driverClass)
        {
            this.driverClass = driverClass;
            return this;
        }

        public Builder setUrl(String url)
        {
            this.url = url;
            return this;
        }

        public Builder setUser(String user)
        {
            this.user = user;
            return this;
        }

        public Builder setPassword(String password)
        {
            this.password = password;
            return this;
        }

        public Builder setPooling(boolean pooling)
        {
            this.pooling = pooling;
            return this;
        }

        public Builder setJar(Optional<String> jar)
        {
            this.jar = jar;
            return this;
        }

        public Builder setPrepareStatements(List<String> prepareStatement)
        {
            this.prepareStatements = prepareStatement;
            return this;
        }

        public Builder setKerberosPrincipal(Optional<String> kerberosPrincipal)
        {
            this.kerberosPrincipal = kerberosPrincipal;
            return this;
        }

        public Builder setKerberosKeytab(Optional<String> kerberosKeytab)
        {
            this.kerberosKeytab = kerberosKeytab;
            return this;
        }

        public JdbcConnectivityParamsState build()
        {
            return new JdbcConnectivityParamsState(
                    name,
                    driverClass,
                    url,
                    user,
                    password,
                    pooling,
                    jar,
                    prepareStatements,
                    kerberosPrincipal,
                    kerberosKeytab
            );
        }
    }
}
