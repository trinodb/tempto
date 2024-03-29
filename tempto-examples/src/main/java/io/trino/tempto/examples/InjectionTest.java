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

import com.google.inject.Inject;
import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.fulfillment.table.ImmutableTablesState;
import io.trino.tempto.fulfillment.table.MutableTablesState;
import org.testng.annotations.Test;

import com.google.inject.name.Named;

import static org.assertj.core.api.Assertions.assertThat;

public class InjectionTest
        extends ProductTest
{
    @Inject
    MutableTablesState mutableTablesState;

    @BeforeMethodWithContext
    @Inject
    public void setUp(
            ImmutableTablesState immutableTablesState,
            @Named("hdfs.username") String hdfsUsername
    )
    {
        testMethodInjection(immutableTablesState, hdfsUsername);
    }

    @Inject
    @Test(groups = "injection")
    public void testInjection()
    {
        assertThat(mutableTablesState).isNotNull();
    }

    @AfterMethodWithContext
    @Inject
    public void tearDown(
            ImmutableTablesState immutableTablesState,
            @Named("hdfs.username") String hdfsUsername
    )
    {
        testMethodInjection(immutableTablesState, hdfsUsername);
    }

    private void testMethodInjection(ImmutableTablesState immutableTablesState, String hdfsUsername)
    {
        assertThat(immutableTablesState).isNotNull();
        assertThat(hdfsUsername).isEqualTo("hdfs");
    }
}
