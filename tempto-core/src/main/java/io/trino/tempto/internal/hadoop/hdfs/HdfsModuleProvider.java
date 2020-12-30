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

package io.prestosql.tempto.internal.hadoop.hdfs;

import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.prestosql.tempto.configuration.Configuration;
import io.prestosql.tempto.hadoop.hdfs.HdfsClient;
import io.prestosql.tempto.initialization.SuiteModuleProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.GeneralSecurityException;
import java.util.Optional;
import java.util.Set;

import static com.google.inject.name.Names.named;
import static io.prestosql.tempto.internal.hadoop.hdfs.WebHdfsClient.CONF_HDFS_PASSWORD_KEY;
import static io.prestosql.tempto.internal.hadoop.hdfs.WebHdfsClient.CONF_HDFS_WEBHDFS_URI_KEY;

public class HdfsModuleProvider
        implements SuiteModuleProvider
{
    private static final Logger logger = LoggerFactory.getLogger(HdfsModuleProvider.class);

    public static final String CONF_TESTS_HDFS_PATH_KEY = "tests.hdfs.path";

    private static final String AUTHENTICATION_SPNEGO = "SPNEGO";
    private static final int NUMBER_OF_HTTP_RETRIES = 3;

    @Override
    public Module getModule(Configuration configuration)
    {
        return new PrivateModule()
        {
            @Override
            protected void configure()
            {
                Set<String> configurationKeys = configuration.listKeys();
                if (!configurationKeys.contains(CONF_HDFS_WEBHDFS_URI_KEY)
                        || !configurationKeys.contains(CONF_TESTS_HDFS_PATH_KEY)) {
                    logger.debug("No HDFS support enabled as '{}' or '{}' is not configured",
                            CONF_HDFS_WEBHDFS_URI_KEY,
                            CONF_TESTS_HDFS_PATH_KEY);
                    return;
                }
                if (!configurationKeys.contains(CONF_HDFS_PASSWORD_KEY)) {
                    bind(Key.get(String.class, named(CONF_HDFS_PASSWORD_KEY))).toInstance("");
                }

                install(httpRequestsExecutorModule());

                bind(HdfsClient.class).to(WebHdfsClient.class).in(Scopes.SINGLETON);
                bind(HdfsDataSourceWriter.class).to(DefaultHdfsDataSourceWriter.class).in(Scopes.SINGLETON);

                expose(HdfsClient.class);
                expose(HdfsDataSourceWriter.class);
            }

            private Module httpRequestsExecutorModule()
            {
                if (spnegoAuthenticationRequired()) {
                    return new SpnegoHttpRequestsExecutor.Module();
                }
                else {
                    return new SimpleHttpRequestsExecutor.Module();
                }
            }

            private boolean spnegoAuthenticationRequired()
            {
                Optional<String> authentication = configuration.getString("hdfs.webhdfs.authentication");
                return authentication.isPresent() && authentication.get().equalsIgnoreCase(AUTHENTICATION_SPNEGO);
            }

            @Inject
            @Provides
            @Singleton
            CloseableHttpClient createHttpClient()
            {
                HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
                httpClientBuilder.setRetryHandler(new DefaultHttpRequestRetryHandler(NUMBER_OF_HTTP_RETRIES, true));
                skipCertificateValidation(httpClientBuilder);
                return httpClientBuilder.build();
            }

            private void skipCertificateValidation(HttpClientBuilder httpClientBuilder)
            {
                try {
                    httpClientBuilder.setSSLSocketFactory(new SSLConnectionSocketFactory(
                            SSLContextBuilder.create()
                                    .loadTrustMaterial(new TrustSelfSignedStrategy())
                                    .build(),
                            new NoopHostnameVerifier()));
                }
                catch (GeneralSecurityException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}
