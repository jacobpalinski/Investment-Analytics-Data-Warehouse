-- Delete records from staging table which have null values
delete from investment_analytics.staging.staging_economic_indicators
where interest_rate is null
or consumer_price_index is null
or unemployment_rate is null
or gdp_growth_rate is null
or consumer_confidence is null;