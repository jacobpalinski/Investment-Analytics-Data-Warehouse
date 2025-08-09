-- Clear previous results for dimension table checks
delete from investment_analytics.data_quality.data_quality_results where table_name = 'FINANCIALS.DIM_PERIOD';

-- Check surrogate key is unique
insert into investment_analytics.data_quality.data_quality_results
select
'Duplicate period_key',
'FINANCIALS.DIM_PERIOD',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from (
select financials_key
from investment_analytics.financials.dim_period
group by period_key
having count(*) > 1
);

-- Check there are only unique fiscal_year and fiscal_quarter combinations
insert into investment_analytics.data_quality.data_quality_results
select
'Duplicate fiscal_year and fiscal_quarter combinations',
'FINANCIALS.DIM_PERIOD',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from (
select fiscal_year, fiscal_quarter
from investment_analytics.financials.dim_period
group by fiscal_year, fiscal_quarter
having count(*) > 1
);