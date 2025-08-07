select 
count(*) from investment_analytics.data_quality.data_quality_results
where table_name = 'ECONOMIC_INDICATORS.DIM_INDICATOR_DATE' and status = 'FAIL';