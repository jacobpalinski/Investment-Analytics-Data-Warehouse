-- Clear previous results for dimension table checks
delete from investment_analytics.data_quality.data_quality_results where table_name = 'CORE.DIM_COMPANY';

-- Check only one current record per CIK
insert into investment_analytics.data_quality.data_quality_results
select
'Multiple Current Rows For Same CIK',
'CORE.DIM_COMPANY',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from (
select cik
from investment_analytics.core.dim_company
where is_current = true
group by cik
having count(*) > 1
);

-- Check no overlapping effective periods for the same CIK
insert into investment_analytics.data_quality.data_quality_results
select
'Overlapping Effective Dates Per CIK',
'CORE.DIM_COMPANY',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from (
select 
d1.cik
from investment_analytics.core.dim_company d1
join investment_analytics.core.dim_company d2
on d1.cik = d2.cik and d1.company_key <> d2.company_key
where d1.effective_start <= coalesce(d2.effective_end, CURRENT_DATE)
and d2.effective_start <= coalesce(d1.effective_end, CURRENT_DATE)
);

-- Check surrogate key is unique
insert into investment_analytics.data_quality.data_quality_results
select
'Duplicate company_key',
'CORE.DIM_COMPANY',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from (
select 
company_key
from investment_analytics.core.dim_company
group by company_key
having count(*) > 1
);

-- Check required fields are not NULL
insert into investment_analytics.data_quality.data_quality_results
select
'Null fields in dimension',
'CORE.DIM_COMPANY',
count(*) as failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from investment_analytics.core.dim_company
where company_key is null or is_current is null;
