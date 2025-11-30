-- Count number of data quality tests that have failed
select 
count(*) from investment_analytics.data_quality.data_quality_results
where table_name = 'ECONOMIC_INDICATORS.FACT_ECONOMIC_INDICATORS' and status = 'FAIL';