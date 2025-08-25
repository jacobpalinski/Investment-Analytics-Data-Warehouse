select 
count(*) from investment_analytics.data_quality.data_quality_results
where table_name = 'SENTIMENT.DIM_SOURCE' and status = 'FAIL';