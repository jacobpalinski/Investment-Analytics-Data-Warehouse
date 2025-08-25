select 
count(*) from investment_analytics.data_quality.data_quality_results
where table_name = 'STAGING.STAGING_COMBINED_WITH_SENTIMENT' and status = 'FAIL';