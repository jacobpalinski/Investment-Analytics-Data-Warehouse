insert into investment_analytics.staging.staging_financials (
    cik,
    currency,
    filing_date,
    financial_statement,
    fiscal_year,
    fiscal_quarter,
    item,
    value
)

select 
cik, 
currency, 
filing_date, 
financial_statement, 
fiscal_year, 
fiscal_quarter, 
'gross_margin',
nullif(revenues, 0) / nullif(gross_profit, 0)
from (
    select
        cik,
        currency,
        filing_date,
        financial_statement,
        fiscal_year,
        fiscal_quarter,
        sum(case when item = 'revenues' then value else 0 end) as revenues,
        sum(case when item = 'gross_profit' then value else 0 end) as gross_profit
        from investment_analytics.staging.staging_financials
        group by cik, currency, filing_date, financial_statement, fiscal_year, fiscal_quarter
)

union all

select 
cik, 
currency, 
filing_date, 
financial_statement, 
fiscal_year, 
fiscal_quarter, 
'debt_to_equity',
nullif(liabilities, 0) / nullif(equity, 0)
from (
    select
        cik,
        currency,
        filing_date,
        financial_statement,
        fiscal_year,
        fiscal_quarter,
        sum(case when item = 'liabilities' then value else 0 end) as liabilities,
        sum(case when item = 'equity' then value else 0 end) as equity
        from investment_analytics.staging.staging_financials
        group by cik, currency, filing_date, financial_statement, fiscal_year, fiscal_quarter
)

union all

select
cik, 
currency, 
filing_date, 
financial_statement, 
fiscal_year, 
fiscal_quarter, 
'current_ratio',
nullif(current_assets, 0) / nullif(current_liabilities, 0)
from (
    select
        cik,
        currency,
        filing_date,
        financial_statement,
        fiscal_year,
        fiscal_quarter,
        sum(case when item = 'current_assets' then value else 0 end) as current_assets,
        sum(case when item = 'current_liabilities' then value else 0 end) as current_liabilities
        from investment_analytics.staging.staging_financials
        group by cik, currency, filing_date, financial_statement, fiscal_year, fiscal_quarter
)

union all

select
cik, 
currency, 
filing_date, 
financial_statement, 
fiscal_year, 
fiscal_quarter, 
'operating_margin',
nullif(operating_income, 0) / nullif(revenues, 0)
from (
    select
        cik,
        currency,
        filing_date,
        financial_statement,
        fiscal_year,
        fiscal_quarter,
        sum(case when item = 'operating_income' then value else 0 end) as operating_income,
        sum(case when item = 'revenues' then value else 0 end) as revenues
        from investment_analytics.staging.staging_financials
        group by cik, currency, filing_date, financial_statement, fiscal_year, fiscal_quarter
)

union all

select
cik, 
currency, 
filing_date, 
financial_statement, 
fiscal_year, 
fiscal_quarter, 
'return_on_equity',
nullif(net_income, 0) / nullif(equity, 0)
from (
    select
        cik,
        currency,
        filing_date,
        financial_statement,
        fiscal_year,
        fiscal_quarter,
        sum(case when item = 'net_income' then value else 0 end) as net_income,
        sum(case when item = 'equity' then value else 0 end) as equity
        from investment_analytics.staging.staging_financials
        group by cik, currency, filing_date, financial_statement, fiscal_year, fiscal_quarter
)

union all

select
cik, 
currency, 
filing_date, 
financial_statement, 
fiscal_year, 
fiscal_quarter, 
'net_margin',
nullif(net_income, 0) / nullif(revenues, 0)
from (
    select
        cik,
        currency,
        filing_date,
        financial_statement,
        fiscal_year,
        fiscal_quarter,
        sum(case when item = 'net_income' then value else 0 end) as net_income,
        sum(case when item = 'revenues' then value else 0 end) as revenues
        from investment_analytics.staging.staging_financials
        group by cik, currency, filing_date, financial_statement, fiscal_year, fiscal_quarter
)

union all

select
cik, 
currency, 
filing_date, 
financial_statement, 
fiscal_year, 
fiscal_quarter, 
'return_on_assets',
nullif(net_income, 0) / nullif(assets, 0)
from (
    select
        cik,
        currency,
        filing_date,
        financial_statement,
        fiscal_year,
        fiscal_quarter,
        sum(case when item = 'net_income' then value else 0 end) as net_income,
        sum(case when item = 'assets' then value else 0 end) as assets
        from investment_analytics.staging.staging_financials
        group by cik, currency, filing_date, financial_statement, fiscal_year, fiscal_quarter
);


