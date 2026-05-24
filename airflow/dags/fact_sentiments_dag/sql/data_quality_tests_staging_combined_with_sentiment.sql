-- Clear previous results for dimension table checks
delete from investment_analytics.data_quality.data_quality_results where table_name = 'STAGING.STAGING_COMBINED_WITH_SENTIMENT';

-- Check for nulls in key fields
insert into investment_analytics.data_quality.data_quality_results
select
'Null rows',
'STAGING.STAGING_COMBINED_WITH_SENTIMENT',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from 
(select
*
from investment_analytics.staging.staging_combined_with_sentiment
where date is null
   or sentiment_category is null
   or ticker_symbol is null 
   or source is null
   or article_sentiment_source is null
   or sentiment_score is null);

-- Check for duplicate rows
insert into investment_analytics.data_quality.data_quality_results
select
'Duplicate rows',
'STAGING.STAGING_COMBINED_WITH_SENTIMENT',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from 
(select 
date,
sentiment_category,
ticker_symbol,
source,
article_sentiment_source,
sentiment_score
from investment_analytics.staging.staging_combined_with_sentiment
group by date, sentiment_category, ticker_symbol, source, article_sentiment_source, sentiment_score
having count(*) > 1);

-- Check there are no sentiment scores outside of the range [0,1]
insert into investment_analytics.data_quality.data_quality_results
select
'Invalid sentiment scores',
'STAGING.STAGING_COMBINED_WITH_SENTIMENT',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from 
(select
*
from investment_analytics.staging.staging_combined_with_sentiment
where sentiment_score < 0 or sentiment_score > 1
);
