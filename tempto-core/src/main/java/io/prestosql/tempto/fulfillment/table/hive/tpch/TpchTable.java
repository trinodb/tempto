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

package io.prestosql.tempto.fulfillment.table.hive.tpch;

/**
 * Enum containing names of all TPCH tables. Moreover it holds reference to
 * {@link io.prestosql.tpch.TpchTable} entity which is used for generating data.
 */
public enum TpchTable
{
    NATION(io.prestosql.tpch.TpchTable.NATION),
    REGION(io.prestosql.tpch.TpchTable.REGION),
    PART(io.prestosql.tpch.TpchTable.PART),
    ORDERS(io.prestosql.tpch.TpchTable.ORDERS),
    CUSTOMER(io.prestosql.tpch.TpchTable.CUSTOMER),
    SUPPLIER(io.prestosql.tpch.TpchTable.SUPPLIER),
    LINE_ITEM(io.prestosql.tpch.TpchTable.LINE_ITEM),
    PART_SUPPLIER(io.prestosql.tpch.TpchTable.PART_SUPPLIER);

    private final io.prestosql.tpch.TpchTable<?> entity;

    TpchTable(io.prestosql.tpch.TpchTable<?> entity)
    {
        this.entity = entity;
    }

    public io.prestosql.tpch.TpchTable<?> entity()
    {
        return entity;
    }
}
