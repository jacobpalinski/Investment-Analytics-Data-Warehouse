select 
count(*) from investment_analytics.data_quality.data_quality_results
where table_name = 'SENTIMENT.FACT_SENTIMENTS' and status = 'FAIL';