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

package io.trino.tempto.fulfillment.table.hive.tpch;

/**
 * Enum containing names of all TPCH tables. Moreover it holds reference to
 * {@link io.trino.tpch.TpchTable} entity which is used for generating data.
 */
public enum TpchTable
{
    NATION(io.trino.tpch.TpchTable.NATION),
    REGION(io.trino.tpch.TpchTable.REGION),
    PART(io.trino.tpch.TpchTable.PART),
    ORDERS(io.trino.tpch.TpchTable.ORDERS),
    CUSTOMER(io.trino.tpch.TpchTable.CUSTOMER),
    SUPPLIER(io.trino.tpch.TpchTable.SUPPLIER),
    LINE_ITEM(io.trino.tpch.TpchTable.LINE_ITEM),
    PART_SUPPLIER(io.trino.tpch.TpchTable.PART_SUPPLIER);

    private final io.trino.tpch.TpchTable<?> entity;

    TpchTable(io.trino.tpch.TpchTable<?> entity)
    {
        this.entity = entity;
    }

    public io.trino.tpch.TpchTable<?> entity()
    {
        return entity;
    }
}
