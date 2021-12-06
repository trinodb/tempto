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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.trino.tempto.hadoop.hdfs.HdfsClient;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.entity.ContentProducer;
import org.apache.http.entity.EntityTemplate;
import org.apache.http.entity.InputStreamEntity;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;
import static org.apache.commons.io.IOUtils.copyLarge;
import static org.apache.http.HttpStatus.SC_CREATED;
import static org.apache.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.http.HttpStatus.SC_OK;
import static org.apache.http.HttpStatus.SC_TEMPORARY_REDIRECT;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * HDFS client based on WebHDFS REST API.
 */
public class WebHdfsClient
        implements HdfsClient
{
    private static final Logger logger = getLogger(WebHdfsClient.class);

    public static final String CONF_HDFS_WEBHDFS_URI_KEY = "hdfs.webhdfs.uri";
    public static final String CONF_HDFS_USERNAME_KEY = "hdfs.username";
    public static final String CONF_HDFS_PASSWORD_KEY = "hdfs.webhdfs.password";

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<Map<String, Object>>() {};

    private final URI uri;
    private final String username;
    private final HttpRequestsExecutor httpRequestsExecutor;

    @Inject
    public WebHdfsClient(
            @Named(CONF_HDFS_WEBHDFS_URI_KEY) String uri,
            @Named(CONF_HDFS_USERNAME_KEY) String username,
            HttpRequestsExecutor httpRequestsExecutor)
    {
        this.uri = URI.create(requireNonNull(uri, "uri is null"));
        this.username = checkNotNull(username, "username is null");
        this.httpRequestsExecutor = checkNotNull(httpRequestsExecutor, "username is null");
    }

    @Override
    public void createDirectory(String path)
    {
        // TODO: reconsider permission=777
        HttpPut mkdirRequest = new HttpPut(buildUri(path, "MKDIRS", ImmutableMap.of("permission", "777")));
        try (CloseableHttpResponse response = httpRequestsExecutor.execute(mkdirRequest)) {
            if (response.getStatusLine().getStatusCode() != SC_OK) {
                throw invalidStatusException("MKDIRS", path, mkdirRequest, response);
            }
            logger.debug("Created directory {} - username: {}", path, username);
        }
        catch (IOException e) {
            throw new RuntimeException("Could not create directory " + path + " in hdfs, user: " + username, e);
        }
    }

    @Override
    public void delete(String path)
    {
        HttpDelete removeFileOrDirectoryRequest = new HttpDelete(buildUri(path, "DELETE", ImmutableMap.of("recursive", "true")));
        try (CloseableHttpResponse response = httpRequestsExecutor.execute(removeFileOrDirectoryRequest)) {
            if (response.getStatusLine().getStatusCode() != SC_OK) {
                throw invalidStatusException("DELETE", path, removeFileOrDirectoryRequest, response);
            }
            logger.debug("Removed file or directory {} - username: {}", path, username);
        }
        catch (IOException e) {
            throw new RuntimeException("Could not remove file or directory " + path + " in hdfs, user: " + username, e);
        }
    }

    @Override
    public void saveFile(String path, InputStream input)
    {
        try {
            saveFile(path, new BufferedHttpEntity(new InputStreamEntity(input)));
        }
        catch (IOException e) {
            throw new RuntimeException("Could not create buffered http entity", e);
        }
    }

    @Override
    public void saveFile(String path, RepeatableContentProducer repeatableContentProducer)
    {
        saveFile(path, new EntityTemplate(toApacheContentProducer(repeatableContentProducer)));
    }

    private ContentProducer toApacheContentProducer(RepeatableContentProducer repeatableContentProducer)
    {
        return (OutputStream outputStream) -> {
            try (InputStream inputStream = repeatableContentProducer.getInputStream()) {
                copyLarge(inputStream, outputStream);
            }
        };
    }

    private void saveFile(String path, HttpEntity entity)
    {
        String writeRedirectUri = executeAndGetRedirectUri(new HttpPut(
                buildUri(path, "CREATE", ImmutableMap.of("overwrite", "true"))));
        HttpPut writeRequest = new HttpPut(writeRedirectUri);
        writeRequest.addHeader("content-type", "application/octet-stream");
        writeRequest.setEntity(entity);

        try (CloseableHttpResponse response = httpRequestsExecutor.execute(writeRequest)) {
            if (response.getStatusLine().getStatusCode() != SC_CREATED) {
                throw invalidStatusException("CREATE", path, writeRequest, response);
            }
            long length = waitForFileSavedAndReturnLength(path);
            logger.debug("Saved file {} - username: {}, size: {}", path, username, byteCountToDisplaySize(length));
        }
        catch (IOException e) {
            throw new RuntimeException("Could not save file " + path + " in hdfs, user: " + username, e);
        }
    }

    @Override
    public void loadFile(String path, OutputStream outputStream)
    {
        HttpGet readRequest = new HttpGet(buildUri(path, "OPEN", ImmutableMap.of()));
        try (CloseableHttpResponse response = httpRequestsExecutor.execute(readRequest)) {
            if (response.getStatusLine().getStatusCode() != SC_OK) {
                throw invalidStatusException("OPEN", path, readRequest, response);
            }

            IOUtils.copy(response.getEntity().getContent(), outputStream);

            logger.debug("Loaded file {} - username: {}", path, username);
        }
        catch (IOException e) {
            throw new RuntimeException("Could not read file " + path + " in hdfs, user: " + username, e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<String> listDirectory(String path)
    {
        HttpGet request = new HttpGet(buildUri(path, "LISTSTATUS", emptyMap()));
        try (CloseableHttpResponse response = httpRequestsExecutor.execute(request)) {
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != SC_OK) {
                throw invalidStatusException("LISTSTATUS", path, request, response);
            }
            Map<String, Object> responseObject = deserializeJsonResponse(response);
            List<Map<String, Object>> fileStatuses = (List<Map<String, Object>>) ((Map<String, Object>) responseObject.get("FileStatuses")).get("FileStatus");
            return fileStatuses.stream()
                    .map(entry -> (String) entry.get("pathSuffix"))
                    .collect(toImmutableList());
        }
        catch (IOException e) {
            throw new RuntimeException("Could not list: " + path + " , user: " + username, e);
        }
    }

    private Object getAttributeValue(String path, String attribute)
    {
        HttpGet readRequest = new HttpGet(buildUri(path, "GETFILESTATUS", emptyMap()));
        try (CloseableHttpResponse response = httpRequestsExecutor.execute(readRequest)) {
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != SC_OK) {
                throw invalidStatusException("GETFILESTATUS", path, readRequest, response);
            }
            Map<String, Object> responseObject = deserializeJsonResponse(response);
            return ((Map<String, Object>) responseObject.get("FileStatus")).get(attribute);
        }
        catch (IOException e) {
            throw new RuntimeException("Could not get file status: " + path + " , user: " + username, e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public long getLength(String path)
    {
        return ((Number) getAttributeValue(path, "length")).longValue();
    }

    @Override
    @SuppressWarnings("unchecked")
    public String getOwner(String path)
    {
        return (String) getAttributeValue(path, "owner");
    }

    @Override
    public void setOwner(String path, String owner) {
        HttpPut request = new HttpPut(buildUri(path, "SETOWNER", ImmutableMap.of("owner", owner)));
        try (CloseableHttpResponse response = httpRequestsExecutor.execute(request)) {
            if (response.getStatusLine().getStatusCode() != SC_OK) {
                throw invalidStatusException("SETPERMISSION", path, request, response);
            }
            logger.debug("Set owner for {} to {}, username: {}", path, owner, username);
        }
        catch (IOException e) {
            throw new RuntimeException("Could not set owner for path: " + path + " in hdfs to " + owner + ", user: " + username, e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public String getGroup(String path)
    {
        return (String) getAttributeValue(path, "group");
    }

    @Override
    public void setGroup(String path, String group) {
        HttpPut request = new HttpPut(buildUri(path, "SETOWNER", ImmutableMap.of("group", group)));
        try (CloseableHttpResponse response = httpRequestsExecutor.execute(request)) {
            if (response.getStatusLine().getStatusCode() != SC_OK) {
                throw invalidStatusException("SETPERMISSION", path, request, response);
            }
            logger.debug("Set group for {} to {}, username: {}", path, group, username);
        }
        catch (IOException e) {
            throw new RuntimeException("Could not set group for path: " + path + " in hdfs to " + group + ", user: " + username, e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public String getPermission(String path)
    {
        return (String) getAttributeValue(path, "permission");
    }

    @Override
    public void setPermission(String path, String octalPermissions) {
        HttpPut request = new HttpPut(buildUri(path, "SETPERMISSION", ImmutableMap.of("permission", octalPermissions)));
        try (CloseableHttpResponse response = httpRequestsExecutor.execute(request)) {
            if (response.getStatusLine().getStatusCode() != SC_OK) {
                throw invalidStatusException("SETPERMISSION", path, request, response);
            }
            logger.debug("Set permission for {} to {}, username: {}", path, octalPermissions, username);
        }
        catch (IOException e) {
            throw new RuntimeException("Could not set permissions for path: " + path + " in hdfs to " + octalPermissions + ", user: " + username, e);
        }
    }

    @Override
    public boolean exist(String path)
    {
        HttpGet readRequest = new HttpGet(buildUri(path, "GETFILESTATUS", emptyMap()));
        try (CloseableHttpResponse response = httpRequestsExecutor.execute(readRequest)) {
            return response.getStatusLine().getStatusCode() == SC_OK;
        }
        catch (IOException e) {
            throw new RuntimeException("Could not get file status: " + path + " , user: " + username, e);
        }
    }

    @Override
    public void setXAttr(String path, String key, String value)
    {
        Map<String, String> params = ImmutableMap.of(
                "xattr.name", key,
                "xattr.value", value,
                "flag", "CREATE"
        );
        HttpPut setXAttrRequest = new HttpPut(buildUri(path, "SETXATTR", params));
        try (CloseableHttpResponse response = httpRequestsExecutor.execute(setXAttrRequest)) {
            if (response.getStatusLine().getStatusCode() != SC_OK) {
                throw invalidStatusException("SETXATTR", path, setXAttrRequest, response);
            }
            logger.debug("Set xAttr {} = {} for {}, username: {}", key, value, path, username);
        }
        catch (IOException e) {
            throw new RuntimeException("Could not set xAttr for path: " + path + " in hdfs, user: " + username, e);
        }
    }

    @Override
    public void removeXAttr(String path, String key)
    {
        HttpPut setXAttrRequest = new HttpPut(buildUri(path, "REMOVEXATTR", ImmutableMap.of("xattr.name", key)));
        try (CloseableHttpResponse response = httpRequestsExecutor.execute(setXAttrRequest)) {
            if (response.getStatusLine().getStatusCode() != SC_OK) {
                throw invalidStatusException("SETXATTR", path, setXAttrRequest, response);
            }
            logger.debug("Remove xAttr {} for {}, username: {}", key, path, username);
        }
        catch (IOException e) {
            throw new RuntimeException("Could not remove xAttr for path: " + path + " in hdfs, user: " + username, e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Optional<String> getXAttr(String path, String key)
    {
        HttpGet setXAttrRequest = new HttpGet(buildUri(path, "GETXATTRS", ImmutableMap.of()));
        try (CloseableHttpResponse response = httpRequestsExecutor.execute(setXAttrRequest)) {
            if (response.getStatusLine().getStatusCode() == SC_NOT_FOUND) {
                return Optional.empty();
            }
            if (response.getStatusLine().getStatusCode() != SC_OK) {
                throw invalidStatusException("GETXATTRS", path, setXAttrRequest, response);
            }

            Map<String, Object> responseObject = deserializeJsonResponse(response);
            Object xAttrs = responseObject.get("XAttrs");
            if (xAttrs == null) {
                return Optional.empty();
            }

            Optional<String> value = ((List<Object>) xAttrs).stream()
                    .map(it -> (Map<String, String>) it)
                    .filter(it -> it.get("name").equals(key))
                    .map(it -> it.get("value"))
                    .findFirst();
            return value;
        }
        catch (IOException e) {
            throw new RuntimeException("Could not get xAttr for path: " + path + " in hdfs, user: " + username, e);
        }
    }

    private String executeAndGetRedirectUri(HttpUriRequest request)
    {
        try (CloseableHttpResponse response = httpRequestsExecutor.execute(request)) {
            if (response.getStatusLine().getStatusCode() != SC_TEMPORARY_REDIRECT) {
                throw new RuntimeException(format("Expected %s redirect for request %s, but got %s: %s",
                        SC_TEMPORARY_REDIRECT,
                        request,
                        response.getStatusLine().getStatusCode(),
                        response));
            }
            return response.getFirstHeader("Location").getValue();
        }
        catch (IOException e) {
            throw new RuntimeException("Could not execute request " + request, e);
        }
    }

    /**
     * There is some wired bug in WebHDFS, which happens for big files. Just after saving such file
     * it is not possible to immediately set xAttr. Calling GETFILESTATUS seems to introduce
     * some synchronization point, so it should be used just after saving file.
     */
    private long waitForFileSavedAndReturnLength(String path)
    {
        return getLength(path);
    }

    private URI buildUri(String path, String operation, Map<String, String> parameters)
    {
        try {
            if (!path.startsWith("/")) {
                path = "/" + path;
            }
            URIBuilder uriBuilder = new URIBuilder(this.uri)
                    .setPath("/webhdfs/v1" + checkNotNull(path))
                    .setParameter("op", checkNotNull(operation));

            for (Entry<String, String> parameter : parameters.entrySet()) {
                uriBuilder.setParameter(parameter.getKey(), parameter.getValue());
            }

            return uriBuilder.build();
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private RuntimeException invalidStatusException(String operation, String path, HttpRequest request, HttpResponse response)
            throws IOException
    {
        return new RuntimeException("Operation " + operation +
                " on file " + path + " failed, user: " + username +
                ", status: " + response.getStatusLine().getStatusCode() + " " + response.getStatusLine().getReasonPhrase() +
                ", content: " + IOUtils.toString(response.getEntity().getContent()) +
                ", request: " + request.getRequestLine().getMethod() + " " + request.getRequestLine().getUri());
    }

    private Map<String, Object> deserializeJsonResponse(HttpResponse response)
            throws IOException
    {
        return MAPPER.readValue(IOUtils.toString(response.getEntity().getContent()), MAP_TYPE_REFERENCE);
    }
}
