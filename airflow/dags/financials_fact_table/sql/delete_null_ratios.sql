-- Delete records with null values for calculated ratios
delete from investment_analytics.staging.staging_financials
where value is null;