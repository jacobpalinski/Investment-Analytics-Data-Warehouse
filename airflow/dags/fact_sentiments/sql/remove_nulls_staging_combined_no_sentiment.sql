-- Delete records from staging table which have null values or empty strings for description
delete from investment_analytics.staging.staging_combined_no_sentiment
where date is null
or title is null
or description is null
or description = ''
or source is null;