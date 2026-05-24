-- Count number of data quality tests that have failed
select 
count(*) from investment_analytics.data_quality.data_quality_results
where table_name = 'FINANCIALS.FACT_FINANCIALS' and status = 'FAIL';