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

package io.trino.tempto.sql;

import io.trino.tempto.query.QueryExecutor;
import io.trino.tempto.sql.view.ViewContextProvider;

import static io.trino.tempto.query.QueryExecutor.defaultQueryExecutor;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;

public final class SqlContexts
{
    /**
     * Helper method designed to be used with lambda expressions containing assertions used
     * on newly created view:
     * <pre>
     *  executeWith(createViewAs("SELECT * FROM nation", view -&gt; {
     *        assertThat(query(format("SELECT * FROM %s", view.getName())))
     *            .hasRowsCount(25);
     *  }));
     * </pre>
     *
     * @param selectSql sql select statement used to create view
     * @return viewContextProvider
     * @deprecated This method uses internally the default query executor which is deprecated. Use {@link #createViewAs(String, QueryExecutor)} instead.
     */
    @Deprecated
    public static ViewContextProvider createViewAs(String selectSql)
    {
        return createViewAs(selectSql, defaultQueryExecutor());
    }

    public static ViewContextProvider createViewAs(String selectSql, QueryExecutor queryExecutor)
    {
        String viewName = generateRandomName("TEST_VIEW_");
        return new ViewContextProvider(viewName, selectSql, queryExecutor);
    }

    public static ViewContextProvider createViewAs(String viewName, String selectSql, QueryExecutor queryExecutor)
    {
        return new ViewContextProvider(viewName, selectSql, queryExecutor);
    }

    private static String generateRandomName(String prefix)
    {
        return prefix + randomAlphanumeric(4);
    }

    private SqlContexts()
    {
    }
}
