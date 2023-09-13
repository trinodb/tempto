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

package io.trino.tempto.internal.hadoop.hdfs;

import com.google.inject.Inject;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.kerberos.KerberosAuthentication;
import org.apache.hc.client5.http.SystemDefaultDnsResolver;
import org.apache.hc.client5.http.auth.AuthSchemeFactory;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.Credentials;
import org.apache.hc.client5.http.auth.KerberosConfig;
import org.apache.hc.client5.http.auth.StandardAuthScheme;
import org.apache.hc.client5.http.classic.methods.HttpUriRequest;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.auth.SPNegoSchemeFactory;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.config.Lookup;
import org.apache.hc.core5.http.config.RegistryBuilder;
import org.apache.hc.core5.http.protocol.HttpContext;

import javax.security.auth.Subject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

;

public class SpnegoHttpRequestsExecutor
        implements HttpRequestsExecutor
{
    public static class Module
            extends PrivateModule
    {
        @Override
        protected void configure()
        {
            bind(HttpRequestsExecutor.class)
                    .to(SpnegoHttpRequestsExecutor.class)
                    .in(Scopes.SINGLETON);
            expose(HttpRequestsExecutor.class);
        }

        @Inject
        @Provides
        @Singleton
        KerberosAuthentication createKerberosAuthentication(Configuration configuration)
        {
            String username = configuration.getStringMandatory("hdfs.username");
            Optional<String> keytab = configuration.getString("hdfs.webhdfs.keytab");
            checkState(keytab.isPresent(), "In order to use SPNEGO authenticated HDFS " +
                    "you must specify keytab location with the 'hdfs.webhdfs.keytab' property");
            return new KerberosAuthentication(username, keytab.get());
        }
    }

    private final CloseableHttpClient httpClient;
    private final KerberosAuthentication kerberosAuthentication;
    private final HttpContext spnegoAwareHttpContext;
    private final boolean useCanonicalHostname;

    @Inject
    public SpnegoHttpRequestsExecutor(
            CloseableHttpClient httpClient,
            KerberosAuthentication kerberosAuthentication,
            Configuration configuration)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.kerberosAuthentication = requireNonNull(kerberosAuthentication, "kerberosAuthentication is null");
        this.spnegoAwareHttpContext = createSpnegoAwareHttpContext();
        this.useCanonicalHostname = configuration.getBoolean("hdfs.webhdfs.spnego_use_canonical_hostname").orElse(false);
    }

    private HttpContext createSpnegoAwareHttpContext()
    {
        HttpClientContext httpContext = HttpClientContext.create();

        KerberosConfig kerberosConfig = KerberosConfig.custom()
                .setStripPort(true)
                .setUseCanonicalHostname(useCanonicalHostname)
                .build();

        Lookup<AuthSchemeFactory> authSchemeRegistry = RegistryBuilder.<AuthSchemeFactory>create()
                .register(StandardAuthScheme.SPNEGO, new SPNegoSchemeFactory(kerberosConfig, new SystemDefaultDnsResolver())).build();
        httpContext.setAuthSchemeRegistry(authSchemeRegistry);

        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(new AuthScope(null, -1), new NullCredentials());
        httpContext.setCredentialsProvider(credentialsProvider);
        return httpContext;
    }

    @Override
    public CloseableHttpResponse execute(HttpUriRequest request)
    {
        Subject authenticationSubject = kerberosAuthentication.authenticate();
        return Subject.doAs(authenticationSubject, (PrivilegedAction<CloseableHttpResponse>) () -> {
            try {
                return httpClient.execute(request, spnegoAwareHttpContext);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private static class NullCredentials
            implements Credentials
    {
        @Override
        public Principal getUserPrincipal()
        {
            return null;
        }

        @Override
        public char[] getPassword()
        {
            return new char[0];
        }
    }
}
