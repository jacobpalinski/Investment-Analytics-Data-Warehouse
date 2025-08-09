-- Clear previous results for dimension table checks
delete from investment_analytics.data_quality.data_quality_results where table_name = 'ECONOMIC_INDICATORS.FACT_ECONOMIC_INDICATORS';

-- Check there are no duplicate records in the fact table
insert into investment_analytics.data_quality.data_quality_results
select
'Duplicate Records',
'ECONOMIC_INDICATORS.FACT_ECONOMIC_INDICATORS',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from (
select
*
from investment_analytics.economic_indicators.fact_economic_indicators
group by economic_indicators_fact_key, indicator_date_key, indicator, value
having count(*) > 1
);

-- Check surrogate key is unique
insert into investment_analytics.data_quality.data_quality_results
select
'Duplicate economic_indicators_fact_key',
'ECONOMIC_INDICATORS.FACT_ECONOMIC_INDICATORS',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from (
select 
economic_indicators_fact_key
from investment_analytics.economic_indicators.fact_economic_indicators
group by economic_indicators_fact_key
having count(*) > 1
);