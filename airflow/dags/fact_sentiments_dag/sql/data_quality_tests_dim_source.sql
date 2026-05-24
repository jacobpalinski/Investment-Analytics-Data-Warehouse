-- Clear previous results for dimension table checks
delete from investment_analytics.data_quality.data_quality_results where table_name = 'SENTIMENT.DIM_SOURCE';

-- Check surrogate key is unique
insert into investment_analytics.data_quality.data_quality_results
select
'Duplicate source_key',
'SENTIMENT.DIM_SOURCE',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from (
select 
source_key
from investment_analytics.sentiment.dim_source
group by source_key
having count(*) > 1
);

-- Check there are no duplicate source_name
insert into investment_analytics.data_quality.data_quality_results
select
'Duplicate source_name',
'SENTIMENT.DIM_SOURCE',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from (
select 
source_name
from investment_analytics.sentiment.dim_source
group by source_name
having count(*) > 1
);