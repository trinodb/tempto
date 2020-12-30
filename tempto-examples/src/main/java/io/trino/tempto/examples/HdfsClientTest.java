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
import com.google.inject.Inject;
import io.trino.tempto.ProductTest;
import io.trino.tempto.hadoop.hdfs.HdfsClient;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class HdfsClientTest
        extends ProductTest
{
    @Inject
    HdfsClient hdfsClient;

    @Inject
    @Test(groups = "hdfs")
    public void testDefaultPath()
    {
        String defaultPath = "/user/hive/warehouse";

        assertThat(hdfsClient.exist(defaultPath)).isTrue();
        assertThat(hdfsClient.getOwner(defaultPath)).isEqualTo("hive");
        assertThat(hdfsClient.getPermission(defaultPath)).isEqualTo("1777");

    }

    @Inject
    @Test(groups = "hdfs")
    public void testMetadataOperations() {
        String testPath = "/user/hive/warehouse/test_metadata";
        hdfsClient.createDirectory(testPath);

        hdfsClient.setPermission(testPath, "0700");
        assertThat(hdfsClient.getPermission(testPath)).isEqualTo("700");
        // reset permission for following testcases
        hdfsClient.setPermission(testPath, "0777");

        hdfsClient.setOwner(testPath, "kenny");
        assertThat(hdfsClient.getOwner(testPath)).isEqualTo("kenny");

        hdfsClient.setGroup(testPath, "orangepeople");
        assertThat(hdfsClient.getGroup(testPath)).isEqualTo("orangepeople");

        hdfsClient.createDirectory(testPath + "/a");
        hdfsClient.createDirectory(testPath + "/b");
        hdfsClient.createDirectory(testPath + "/c");
        assertThat(hdfsClient.listDirectory(testPath)).containsAll(ImmutableList.of("a", "b", "c"));
    }
}
