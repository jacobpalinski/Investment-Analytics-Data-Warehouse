-- Clear previous results for dimension table checks
delete from investment_analytics.data_quality.data_quality_results where table_name = 'ANALYTICS.DIM_ECONOMIC_INDICATORS';

-- Check only one current record for a given year, quarter and month combination
insert into investment_analytics.data_quality.data_quality_results
select
'Multiple Current Rows For Given Year, Quarter And Month',
'ANALYTICS.DIM_ECONOMIC_INDICATORS',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from (
select
year,
quarter,
month
from investment_analytics.analytics.dim_economic_indicators
where is_current = true
group by year, quarter, month
having count(*) > 1
);

-- Check no overlapping effective periods for the same year, quarter and month combination
insert into investment_analytics.data_quality.data_quality_results
select
'Overlapping Effective Dates Per Year, Quarter And Month',
'ANALYTICS.DIM_ECONOMIC_INDICATORS',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from (
select
d1.year,
d1.month,
d1.quarter
from investment_analytics.analytics.dim_economic_indicators d1
join investment_analytics.analytics.dim_economic_indicators d2
on d1.year = d2.year
and d1.month = d2.month
and d1.quarter = d2.quarter
and d1.economic_indicators_key <> d2.economic_indicators_key
where coalesce(d1.effective_end, '9999-12-31') > d2.effective_start
and d1.effective_start < coalesce(d2.effective_end, '9999-12-31')
);

-- Check surrogate key is unique
insert into investment_analytics.data_quality.data_quality_results
select
'Duplicate economic_indicators_key',
'ANALYTICS.DIM_ECONOMIC_INDICATORS',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from (
select economic_indicators_key
from investment_analytics.analytics.dim_economic_indicators
group by economic_indicators_key
having count(*) > 1
);