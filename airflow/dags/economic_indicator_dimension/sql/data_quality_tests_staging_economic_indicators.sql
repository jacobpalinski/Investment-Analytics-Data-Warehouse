-- Clear previous results for dimension table checks
delete from investment_analytics.data_quality.data_quality_results where table_name = 'ANALYTICS.STAGING_ECONOMIC_INDICATORS';

-- Check for nulls in key fields
insert into investment_analytics.data_quality.data_quality_results
select
'Null rows',
'ANALYTICS.STAGING_ECONOMIC_INDICATORS',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from 
(select
*
from investment_analytics.staging.staging_economic_indicators
where interest_rate is null
or consumer_price_index is null
or unemployment_rate is null 
or gdp_growth_rate is null 
or consumer_confidence is null
or year is null
or quarter is null
or month is null);

-- Check for duplicate rows
insert into investment_analytics.data_quality.data_quality_results
select
'Duplicate rows',
'ANALYTICS.STAGING_FINANCIALS',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from 
(select year, quarter, month, interest_rate, consumer_price_index, unemployment_rate, gdp_growth_rate, consumer_confidence
from investment_analytics.staging.staging_economic_indicators
group by year, quarter, month, interest_rate, consumer_price_index, unemployment_rate, gdp_growth_rate, consumer_confidence
having count(*) > 1);

-- Check for any unexpected negative values
insert into investment_analytics.data_quality.data_quality_results
select
'Unexpected negative values',
'ANALYTICS.STAGING_ECONOMIC_INDICATORS',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from 
(select
*
from investment_analytics.staging.staging_economic_indicators
where consumer_price_index < 0
or unemployment_rate < 0
or consumer_confidence < 0);

-- Check there are no invalid months and quarters
insert into investment_analytics.data_quality.data_quality_results
select
'Invalid Months and Quarters',
'ANALYTICS.STAGING_ECONOMIC_INDICATORS',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from 
(select
*
from investment_analytics.staging.staging_economic_indicators
where month not in ('January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December')
or quarter not in ('Q1', 'Q2', 'Q3', 'Q4'));