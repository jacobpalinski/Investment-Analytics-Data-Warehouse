-- Count number of data quality tests that have failed
select 
count(*) from investment_analytics.data_quality.data_quality_results
where table_name = 'SENTIMENT.DIM_SENTIMENT_DATE' and status = 'FAIL';
