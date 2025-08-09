-- Delete records from staging table which have null value column, extremely large or small value in values column, unexpected negative values for specific items, invalid fiscal_quarter or fiscal_year or invalid filing_date
delete from investment_analytics.staging.staging_financials
where value is null
or (value > 1e12 or value < -1e12)
or (item in ('revenues', 'assets', 'liabilities', 'current_assets', 'current_liabilities', 'noncurrent_liabilities')
and value < 0)
or fiscal_year > extract(year from current_date)
or fiscal_quarter not in ('Q1', 'Q2', 'Q3', 'Annual')
or filing_date > current_date;