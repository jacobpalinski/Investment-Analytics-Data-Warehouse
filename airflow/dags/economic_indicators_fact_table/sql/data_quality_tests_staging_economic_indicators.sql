-- Clear previous results for dimension table checks
delete from investment_analytics.data_quality.data_quality_results where table_name = 'STAGING.STAGING_ECONOMIC_INDICATORS';

-- Check for nulls in key fields
insert into investment_analytics.data_quality.data_quality_results
select
'Null rows',
'STAGING.STAGING_ECONOMIC_INDICATORS',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from 
(select
*
from investment_analytics.staging.staging_economic_indicators
where indicator is null
or value is null
or year is null
or quarter is null
or month is null);

-- Check for duplicate rows
insert into investment_analytics.data_quality.data_quality_results
select
'Duplicate rows',
'STAGING.STAGING_ECONOMIC_INDICATORS',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from 
(select year, quarter, month, day, indicator, value
from investment_analytics.staging.staging_economic_indicators
group by year, quarter, month, day, indicator, value
having count(*) > 1);

-- Check for any unexpected negative values
insert into investment_analytics.data_quality.data_quality_results
select
'Unexpected negative values',
'STAGING.STAGING_ECONOMIC_INDICATORS',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from 
(select
*
from investment_analytics.staging.staging_economic_indicators
where (indicator = 'consumer_price_index' and value < 0)
or (indicator = 'unemployment_rate' and value < 0)
or (indicator = 'consumer_confidence' and value < 0));