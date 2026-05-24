-- Check there are no duplicate records in dim_indicator_date table
insert into investment_analytics.data_quality.data_quality_results
select
'Duplicate Records',
'ECONOMIC_INDICATORS.DIM_INDICATOR_DATE',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from (
select
date,
year,
quarter,
month,
day
from investment_analytics.economic_indicators.dim_indicator_date
group by date, year, quarter, month, day
having count(*) > 1
);

-- Check primary key is unique
select
'Duplicate indicator_date_key',
'ECONOMIC_INDICATORS.DIM_INDICATOR_DATE',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from (
select
indicator_date_key
from investment_analytics.economic_indicators.dim_indicator_date
group by indicator_date_key
having count(*) > 1
);
