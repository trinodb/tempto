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

import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.ProductTest;
import org.testng.annotations.Test;

import static io.trino.tempto.internal.fulfillment.resources.SuiteResourcesState.closeAfterSuite;
import static io.trino.tempto.internal.fulfillment.resources.TestResourcesState.closeAfterTest;
import static org.assertj.core.api.Assertions.assertThat;

public class ResourcesTest
        extends ProductTest
{
    private boolean testResource;
    private boolean suiteResource;

    @BeforeTestWithContext
    public void suiteResources()
    {
        suiteResource = true;
        closeAfterSuite(() -> suiteResource = false);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (suiteResource) {
                System.err.println("Suite resource is still allocated");
                System.exit(1);
            }
        }));
    }

    @Test
    public void testLevelResource()
    {
        assertThat(testResource).as("Resources is allocated").isFalse();
        testResource = true;
        closeAfterTest(() -> testResource = false);
    }

    @Test
    public void anotherTestLevelResource()
    {
        assertThat(testResource).as("Resources is allocated").isFalse();
        testResource = true;
        closeAfterTest(() -> testResource = false);
    }
}
