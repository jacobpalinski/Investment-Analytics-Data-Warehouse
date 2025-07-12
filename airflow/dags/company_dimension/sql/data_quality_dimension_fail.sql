select 
count(*) from investment_analytics.data_quality.data_quality_results
where table_name = 'ANALYTICS.DIM_COMPANY' and status = 'FAIL';