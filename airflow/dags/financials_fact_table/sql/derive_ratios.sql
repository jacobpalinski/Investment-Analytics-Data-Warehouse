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
denominator.cik,
denominator.currency,
denominator.filing_date,
'metrics' as financial_statement,
denominator.fiscal_year,
denominator.fiscal_quarter,
'gross_margin' as item,
case
when denominator.value != 0 then numerator.value / denominator.value
else null
end as value
from investment_analytics.staging.staging_financials as denominator
join investment_analytics.staging.staging_financials as numerator
on denominator.cik = numerator.cik
and denominator.filing_date = numerator.filing_date
and denominator.fiscal_year = numerator.fiscal_year
and denominator.fiscal_quarter = numerator.fiscal_quarter
and denominator.currency = numerator.currency
and denominator.financial_statement = 'income_statement'
and numerator.financial_statement = 'income_statement'
where denominator.item = 'revenues'
and numerator.item = 'gross_profit'

union all

select
denominator.cik,
denominator.currency,
denominator.filing_date,
'metrics' as financial_statement,
denominator.fiscal_year,
denominator.fiscal_quarter,
'debt_to_equity' as item,
case
when denominator.value != 0 then numerator.value / denominator.value
else null
end as value
from investment_analytics.staging.staging_financials as denominator
join investment_analytics.staging.staging_financials as numerator
on denominator.cik = numerator.cik
and denominator.filing_date = numerator.filing_date
and denominator.fiscal_year = numerator.fiscal_year
and denominator.fiscal_quarter = numerator.fiscal_quarter
and denominator.currency = numerator.currency
and denominator.financial_statement = 'balance_sheet'
and numerator.financial_statement = 'balance_sheet'
where denominator.item = 'equity'
and numerator.item = 'liabilities'

union all

select
denominator.cik,
denominator.currency,
denominator.filing_date,
'metrics' as financial_statement,
denominator.fiscal_year,
denominator.fiscal_quarter,
'current_ratio' as item,
case
when denominator.value != 0 then numerator.value / denominator.value
else null
end as value
from investment_analytics.staging.staging_financials as denominator
join investment_analytics.staging.staging_financials as numerator
on denominator.cik = numerator.cik
and denominator.filing_date = numerator.filing_date
and denominator.fiscal_year = numerator.fiscal_year
and denominator.fiscal_quarter = numerator.fiscal_quarter
and denominator.currency = numerator.currency
and denominator.financial_statement = 'balance_sheet'
and numerator.financial_statement = 'balance_sheet'
where denominator.item = 'current_liabilities'
and numerator.item = 'current_assets'

union all

select
denominator.cik,
denominator.currency,
denominator.filing_date,
'metrics' as financial_statement,
denominator.fiscal_year,
denominator.fiscal_quarter,
'operating_margin' as item,
case
when denominator.value != 0 then numerator.value / denominator.value
else null
end as value
from investment_analytics.staging.staging_financials as denominator
join investment_analytics.staging.staging_financials as numerator
on denominator.cik = numerator.cik
and denominator.filing_date = numerator.filing_date
and denominator.fiscal_year = numerator.fiscal_year
and denominator.fiscal_quarter = numerator.fiscal_quarter
and denominator.currency = numerator.currency
and denominator.financial_statement = 'income_statement'
and numerator.financial_statement = 'income_statement'
where denominator.item = 'revenues'
and numerator.item = 'operating_income'

union all

select
denominator.cik,
denominator.currency,
denominator.filing_date,
'metrics' as financial_statement,
denominator.fiscal_year,
denominator.fiscal_quarter,
'return_on_equity' as item,
case
when denominator.value != 0 then numerator.value / denominator.value
else null
end as value
from investment_analytics.staging.staging_financials as denominator
join investment_analytics.staging.staging_financials as numerator
on denominator.cik = numerator.cik
and denominator.filing_date = numerator.filing_date
and denominator.fiscal_year = numerator.fiscal_year
and denominator.fiscal_quarter = numerator.fiscal_quarter
and denominator.currency = numerator.currency
and denominator.financial_statement = 'balance_sheet'
and numerator.financial_statement = 'income_statement'
where denominator.item = 'equity'
and numerator.item = 'net_income'

union all

select
denominator.cik,
denominator.currency,
denominator.filing_date,
'metrics' as financial_statement,
denominator.fiscal_year,
denominator.fiscal_quarter,
'net_margin' as item,
case
when denominator.value != 0 then numerator.value / denominator.value
else null
end as value
from investment_analytics.staging.staging_financials as denominator
join investment_analytics.staging.staging_financials as numerator
on denominator.cik = numerator.cik
and denominator.filing_date = numerator.filing_date
and denominator.fiscal_year = numerator.fiscal_year
and denominator.fiscal_quarter = numerator.fiscal_quarter
and denominator.currency = numerator.currency
and denominator.financial_statement = 'income_statement'
and numerator.financial_statement = 'income_statement'
where denominator.item = 'revenues'
and numerator.item = 'net_income'

union all

select
denominator.cik,
denominator.currency,
denominator.filing_date,
'metrics' as financial_statement,
denominator.fiscal_year,
denominator.fiscal_quarter,
'return_on_assets' as item,
case
when denominator.value != 0 then numerator.value / denominator.value
else null
end as value
from investment_analytics.staging.staging_financials as denominator
join investment_analytics.staging.staging_financials as numerator
on denominator.cik = numerator.cik
and denominator.filing_date = numerator.filing_date
and denominator.fiscal_year = numerator.fiscal_year
and denominator.fiscal_quarter = numerator.fiscal_quarter
and denominator.currency = numerator.currency
and denominator.financial_statement = 'balance_sheet'
and numerator.financial_statement = 'income_statement'
where denominator.item = 'assets'
and numerator.item = 'net_income';


