-- database: presto; groups: stats, tpcds; tables: tpcds.call_center 
-- delimiter: |; ignoreOrder: true; types: VARCHAR|DOUBLE|DOUBLE|DOUBLE|DOUBLE|VARCHAR|VARCHAR
--! name: show stats for call_center
SHOW STATS FOR hive.tpcds.call_center
--!
cc_call_center_sk|null|6.00000000000|0.00000000000|null|1|6|
cc_call_center_id|32.00000000000|3.00000000000|0.00000000000|null|null|null|
cc_rec_start_date|null|4.00000000000|0.00000000000|null|1998-01-01|2002-01-01|
cc_rec_end_date|null|2.00000000000|0.50000000000|null|2000-01-01|2001-12-31|
cc_closed_date_sk|null|0.00000000000|1.00000000000|null|null|null|
cc_open_date_sk|null|3.00000000000|0.00000000000|null|2450806|2451063|
cc_name|100.00000000000|3.00000000000|0.00000000000|null|null|null|
cc_class|100.00000000000|3.00000000000|0.00000000000|null|null|null|
cc_employees|null|5.00000000000|0.00000000000|null|1|7|
cc_sq_ft|null|6.00000000000|0.00000000000|null|649|4134|
cc_hours|40.00000000000|2.00000000000|0.00000000000|null|null|null|
cc_manager|80.00000000000|4.00000000000|0.00000000000|null|null|null|
cc_mkt_id|null|3.00000000000|0.00000000000|null|2|6|
cc_mkt_class|100.00000000000|5.00000000000|0.00000000000|null|null|null|
cc_mkt_desc|200.00000000000|4.00000000000|0.00000000000|null|null|null|
cc_market_manager|80.00000000000|4.00000000000|0.00000000000|null|null|null|
cc_division|null|4.00000000000|0.00000000000|null|1|5|
cc_division_name|100.00000000000|4.00000000000|0.00000000000|null|null|null|
cc_company|null|4.00000000000|0.00000000000|null|1|6|
cc_company_name|100.00000000000|4.00000000000|0.00000000000|null|null|null|
cc_street_number|20.00000000000|3.00000000000|0.00000000000|null|null|null|
cc_street_name|120.00000000000|3.00000000000|0.00000000000|null|null|null|
cc_street_type|30.00000000000|3.00000000000|0.00000000000|null|null|null|
cc_suite_number|20.00000000000|3.00000000000|0.00000000000|null|null|null|
cc_city|120.00000000000|1.00000000000|0.00000000000|null|null|null|
cc_county|60.00000000000|1.00000000000|0.00000000000|null|null|null|
cc_state|4.00000000000|1.00000000000|0.00000000000|null|null|null|
cc_zip|20.00000000000|1.00000000000|0.00000000000|null|null|null|
cc_country|40.00000000000|1.00000000000|0.00000000000|null|null|null|
cc_gmt_offset|null|1.00000000000|0.00000000000|null|-10.0|-10.0|
cc_tax_percentage|null|4.00000000000|0.00000000000|null|0.02|0.24|
null|null|null|null|6.00000000000|null|null|
