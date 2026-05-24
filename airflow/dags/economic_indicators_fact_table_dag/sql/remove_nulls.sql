-- Delete records from staging table which have null values
delete from investment_analytics.staging.staging_economic_indicators
where value is null;