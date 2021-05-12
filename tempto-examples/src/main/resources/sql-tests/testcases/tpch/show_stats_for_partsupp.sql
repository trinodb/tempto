-- database: trino; groups: stats, tpch; tables: partsupp
-- delimiter: |; ignoreOrder: true; types: VARCHAR|DOUBLE|DOUBLE|DOUBLE|DOUBLE|VARCHAR|VARCHAR
--! name: show stats for partsupp
SHOW STATS FOR partsupp
--!
ps_partkey|null|200000.00000000000|0.00000000000|null|1|200000|
ps_suppkey|null|10000.00000000000|0.00000000000|null|1|10000|
ps_availqty|null|9999.00000000000|0.00000000000|null|1|9999|
ps_supplycost|null|1999.00000000000|0.00000000000|null|0.02|20.0|
ps_comment|53066639.99999999000|799124.00000000000|0.00000000000|null|null|null|
null|null|null|null|800000.00000000000|null|null|
