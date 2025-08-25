-- Clear previous results for dimension table checks
delete from investment_analytics.data_quality.data_quality_results where table_name = 'SENTIMENT.DIM_SENTIMENT_DATE';

-- Check surrogate key is unique
insert into investment_analytics.data_quality.data_quality_results
select
'Duplicate sentiment_date_key',
'SENTIMENT.DIM_SENTIMENT_DATE',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from (
select 
sentiment_date_key
from investment_analytics.sentiment.dim_sentiment_date
group by sentiment_date_key
having count(*) > 1
);

-- Check there are no duplicate date, day_of_week, day, month, quarter and year combinations
insert into investment_analytics.data_quality.data_quality_results
select
'Duplicate date combinations',
'SENTIMENT.DIM_SENTIMENT_DATE',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from (
select 
date,
day_of_week,
day,
month,
quarter,
year
from investment_analytics.sentiment.dim_sentiment_date
group by date, day_of_week, day, month, quarter, year
having count(*) > 1
);