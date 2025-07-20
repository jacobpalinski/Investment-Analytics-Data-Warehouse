update investment_analytics.staging.staging_financials
set fiscal_quarter = 'Annual'
where fiscal_year is not null and fiscal_quarter is null;