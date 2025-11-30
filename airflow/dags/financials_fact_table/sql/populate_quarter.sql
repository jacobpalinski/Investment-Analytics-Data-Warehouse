-- For annual filings set fiscal_quarter to Q4
update investment_analytics.staging.staging_financials
set fiscal_quarter = 'Q4'
where fiscal_year is not null and fiscal_quarter is null;