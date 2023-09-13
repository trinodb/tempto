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

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Scopes;
import com.google.inject.name.Named;
import org.apache.hc.client5.http.classic.methods.HttpUriRequest;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.net.URIBuilder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static com.google.common.io.BaseEncoding.base64;
import static io.trino.tempto.internal.hadoop.hdfs.WebHdfsClient.CONF_HDFS_PASSWORD_KEY;
import static io.trino.tempto.internal.hadoop.hdfs.WebHdfsClient.CONF_HDFS_USERNAME_KEY;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.util.Objects.requireNonNull;

public class SimpleHttpRequestsExecutor
        implements HttpRequestsExecutor
{
    public static class Module
            extends AbstractModule
    {
        @Override
        public void configure()
        {
            bind(HttpRequestsExecutor.class)
                    .to(SimpleHttpRequestsExecutor.class)
                    .in(Scopes.SINGLETON);
        }
    }

    private final CloseableHttpClient httpClient;
    private final String username;
    private final String password;

    @Inject
    public SimpleHttpRequestsExecutor(
            CloseableHttpClient httpClient,
            @Named(CONF_HDFS_USERNAME_KEY) String username,
            @Named(CONF_HDFS_PASSWORD_KEY) String password)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.username = requireNonNull(username, "username is null");
        this.password = requireNonNull(password, "password is null");
    }

    @Override
    public CloseableHttpResponse execute(HttpUriRequest request)
            throws IOException
    {
        HttpUriRequest usernameContainingRequest = appendUsernameToQueryString(request);
        return httpClient.execute(usernameContainingRequest);
    }

    private HttpUriRequest appendUsernameToQueryString(HttpUriRequest request)
    {
        try {
            URI originalUri = request.getUri();
            URI uriWithUsername = appendUsername(originalUri);
            request.setUri(uriWithUsername);
            if (!password.isEmpty()) {
                request.setHeader("Authorization", "Basic " + base64().encode(format("%s:%s", username, password).getBytes(ISO_8859_1)));
            }
            return request;
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private URI appendUsername(URI originalUri)
    {
        URIBuilder uriBuilder = new URIBuilder(originalUri);
        uriBuilder.setParameter("user.name", username);
        try {
            return uriBuilder.build();
        }
        catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
