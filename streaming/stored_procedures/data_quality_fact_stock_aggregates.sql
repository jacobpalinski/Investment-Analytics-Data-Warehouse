CREATE OR REPLACE PROCEDURE INVESTMENT_ANALYTICS.STAGING.DATA_QUALITY_FACT_STOCK_AGGREGATES()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS 'begin

-- Clear previous results for dimension table checks
delete from investment_analytics.data_quality.data_quality_results where table_name = ''STOCKS.FACT_STOCK_AGGREGATES'';

-- Check surrogate key is unique
insert into investment_analytics.data_quality.data_quality_results
select
''Duplicate stock_aggregates_fact_key'',
''STOCKS.FACT_STOCK_AGGREGATES'',
count(*) AS failed_count,
case when count(*) = 0 then ''PASS'' else ''FAIL'' end,
current_timestamp
from (
select stock_aggregates_fact_key
from investment_analytics.stocks.fact_stock_aggregates
group by stock_aggregates_fact_key
having count(*) > 1
);

-- Check there are no rows with null records
insert into investment_analytics.data_quality.data_quality_results
select
''Null Records'',
''STOCKS.FACT_STOCK_AGGREGATES'',
count(*) AS failed_count,
case when count(*) = 0 then ''PASS'' else ''FAIL'' end,
current_timestamp
from
(
select
stock_aggregates_fact_key,
date_key,
company_key,
volume,
accumulated_volume,
volume_weighted_average_price,
closing_price,
average_trade_size
from investment_analytics.stocks.fact_stock_aggregates
where stock_aggregates_fact_key is null
or date_key is null
or company_key is null
or volume is null
or accumulated_volume is null
or volume_weighted_average_price is null
or closing_price is null
or average_trade_size is null);

-- Check there are no duplicate records
insert into investment_analytics.data_quality.data_quality_results
select
''Duplicate Records'',
''STOCKS.FACT_STOCK_AGGREGATES'',
count(*) AS failed_count,
case when count(*) = 0 then ''PASS'' else ''FAIL'' end,
current_timestamp
from
(
select
stock_aggregates_fact_key,
date_key,
company_key,
volume,
accumulated_volume,
volume_weighted_average_price,
closing_price,
average_trade_size
from investment_analytics.stocks.fact_stock_aggregates
group by stock_aggregates_fact_key, date_key, company_key, volume, accumulated_volume, volume_weighted_average_price, closing_price, average_trade_size
having count(*) > 1
);

-- Check there are no rows with invalid volume, accumulated_volume, volume_weighted_average_price, closing_price, average_size values
insert into investment_analytics.data_quality.data_quality_results
select
''Invalid Records'',
''STOCKS.FACT_STOCK_AGGREGATES'',
count(*) AS failed_count,
case when count(*) = 0 then ''PASS'' else ''FAIL'' end,
current_timestamp
from
(
select
volume,
accumulated_volume,
volume_weighted_average_price,
closing_price,
average_trade_size
from investment_analytics.stocks.fact_stock_aggregates
where volume < 0
or accumulated_volume < 0
or volume_weighted_average_price < 0
or closing_price < 0
or average_trade_size < 0
);
return ''data_quality_fact_stock_aggregates successfully completed'';
end';