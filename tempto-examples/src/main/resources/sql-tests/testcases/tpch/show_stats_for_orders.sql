-- database: trino; groups: stats, tpcds; tables: orders
-- delimiter: |; ignoreOrder: true; types: VARCHAR|DOUBLE|DOUBLE|DOUBLE|DOUBLE|VARCHAR|VARCHAR
--! name: show stats for orders 
SHOW STATS FOR orders
--!
o_orderkey|null|1500000.00000000000|0.00000000000|null|1|6000000|
o_custkey|null|99996.00000000000|0.00000000000|null|1|149999|
o_orderstatus|499950.00000000000|3.00000000000|0.00000000000|null|null|null|
o_totalprice|null|1108856.00000000000|0.00000000000|null|17.15|11105.7|
o_orderdate|null|2406.00000000000|0.00000000000|null|1992-01-01|1998-08-02|
o_orderpriority|7500000.00000000000|5.00000000000|0.00000000000|null|null|null|
o_clerk|7500000.00000000000|1000.00000000000|0.00000000000|null|null|null|
o_shippriority|null|1.00000000000|0.00000000000|null|0|0|
o_comment|39499950.00000000000|1482071.00000000000|0.00000000000|null|null|null|
null|null|null|null|1500000.00000000000|null|null|
