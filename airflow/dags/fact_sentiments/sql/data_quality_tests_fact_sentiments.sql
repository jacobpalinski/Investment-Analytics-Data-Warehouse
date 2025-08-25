-- Clear previous results for dimension table checks
delete from investment_analytics.data_quality.data_quality_results where table_name = 'SENTIMENT.FACT_SENTIMENTS';

-- Check surrogate key is unique
insert into investment_analytics.data_quality.data_quality_results
select
'Duplicate sentiments_fact_key',
'SENTIMENT.FACT_SENTIMENTS',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from (
select 
sentiments_fact_key
from investment_analytics.sentiment.fact_sentiments
group by sentiments_fact_key
having count(*) > 1
);

-- Check there are no null values for sentiment_date_key, source_key and company_key
insert into investment_analytics.data_quality.data_quality_results
select
'Null Surrogate Keys',
'SENTIMENT.FACT_SENTIMENTS',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from
(select
sentiment_date_key,
source_key,
company_key
from investment_analytics.sentiment.fact_sentiments
where sentiment_date_key is null
or source_key is null
or company_key is null);

-- Check there are no duplicate sentiment_date_key, source_key, company_key, sentiment_category, article_sentiment_source and sentiment_score combinations
insert into investment_analytics.data_quality.data_quality_results
select
'Duplicate Sentiment Entries',
'SENTIMENT.FACT_SENTIMENTS',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from 
(select
sentiment_date_key,
source_key,
company_key,
sentiment_category,
article_sentiment_source,
sentiment_score
from investment_analytics.sentiment.fact_sentiments
group by sentiment_date_key, source_key, company_key, sentiment_category, article_sentiment_source, sentiment_score
having count(*) > 1
)