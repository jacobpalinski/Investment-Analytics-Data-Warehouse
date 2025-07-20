-- Clear previous results for dimension table checks
delete from investment_analytics.data_quality.data_quality_results where table_name = 'ANALYTICS.DIM_FINANCIALS';

-- Check surrogate key is unique
insert into investment_analytics.data_quality.data_quality_results
select
'Duplicate financials_key',
'ANALYTICS.DIM_FINANCIALS',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from (
select financials_key
from investment_analytics.analytics.dim_financials
group by company_key
having count(*) > 1
);

-- Check there are no ciks inside of the DIM_COMPANY that are not inside of DIM_FINANCIALS
insert into investment_analytics.data_quality.data_quality_results
select
"Missing cik's",
'ANALYTICS.STAGING_FINANCIALS',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from
(select
dim_company.cik
from investment_analytics.analytics.dim_company as dim_company
left join investment_analytics.analytics.dim_financials as dim_financials
on dim_company.cik = dim_financials.cik
where dim_financials.cik is null)



