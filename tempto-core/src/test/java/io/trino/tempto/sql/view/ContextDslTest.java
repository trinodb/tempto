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

package io.trino.tempto.sql.view;

import io.trino.tempto.context.ContextDsl;
import io.trino.tempto.context.ContextRunnable;
import io.trino.tempto.query.QueryExecutor;
import io.trino.tempto.query.QueryResult;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static io.trino.tempto.internal.configuration.TestConfigurationFactory.TEST_CONFIGURATION_URIS_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ContextDslTest
{
    @BeforeAll
    public static void setupSpec()
    {
        System.setProperty(TEST_CONFIGURATION_URIS_KEY, "/configuration/global-configuration-tempto.yaml");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void executeWithView()
    {
        String viewName = "test_view";
        String selectSql = "SELECT * FROM nation";
        ContextRunnable<View> testRunnable = mock(ContextRunnable.class);
        QueryExecutor queryExecutor = mock(QueryExecutor.class);
        lenient().when(queryExecutor.executeQuery("DROP VIEW test_view")).thenReturn(QueryResult.forSingleIntegerValue(1));
        lenient().when(queryExecutor.executeQuery("CREATE VIEW test_view AS SELECT * FROM nation")).thenReturn(QueryResult.forSingleIntegerValue(1));

        ViewContextProvider contextProvider = new ViewContextProvider(viewName, selectSql, queryExecutor);

        ContextDsl.executeWith(contextProvider, testRunnable);

        verify(testRunnable).run(any());
    }
}
