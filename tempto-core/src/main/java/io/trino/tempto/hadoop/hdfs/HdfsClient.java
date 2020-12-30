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

package io.prestosql.tempto.hadoop.hdfs;

import org.apache.commons.io.output.ByteArrayOutputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Optional;

/**
 * HDFS client.
 */
public interface HdfsClient
{
    /**
     * Interface of an object that can open same input stream multiple times.
     */
    @FunctionalInterface
    interface RepeatableContentProducer
    {
        /**
         * @return a content {@link InputStream}. It will be automatically closed after use.
         * @throws IOException If the stream cannot be opened
         */
        InputStream getInputStream()
                throws IOException;
    }

    void createDirectory(String path);

    void delete(String path);

    void saveFile(String path, InputStream input);

    void saveFile(String path, RepeatableContentProducer repeatableContentProducer);

    default void saveFile(String path, String content)
    {
        saveFile(path, new ByteArrayInputStream(content.getBytes()));
    }

    void loadFile(String path, OutputStream outputStream);

    List<String> listDirectory(String path);

    default String loadFile(String path, Charset charset)
    {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        loadFile(path, output);
        return new String(output.toByteArray(), charset);
    }

    void setXAttr(String path, String key, String value);

    void removeXAttr(String path, String key);

    Optional<String> getXAttr(String path, String key);

    /**
     * @param path File to be examined
     * @return length of a file stored in HDFS, -1 if file not exists
     */
    long getLength(String path);

    boolean exist(String path);

    String getOwner(String path);

    void setOwner(String path, String owner);

    String getGroup(String path);

    void setGroup(String path, String group);

    String getPermission(String path);

    void setPermission(String path, String octalPermissions);
}
