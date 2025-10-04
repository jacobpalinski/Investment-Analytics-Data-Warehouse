insert into investment_analytics.staging.staging_financials (
    cik,
    filing_date,
    financial_statement,
    fiscal_year,
    fiscal_quarter,
    item,
    usd_value
)

with row_num_filing_date as (
    select
        cik,
        filing_date,
        financial_statement,
        fiscal_year,
        fiscal_quarter,
        item,
        usd_value,
        row_number() over (
            partition by cik, fiscal_year, fiscal_quarter, item
            order by filing_date desc
        ) as rn
    from investment_analytics.staging.staging_financials
),

most_recent_staging as (
    select *
    from row_num_filing_date
    where rn = 1
),

metrics as (
select
denominator.cik,
denominator.filing_date,
'metrics' as financial_statement,
denominator.fiscal_year,
denominator.fiscal_quarter,
'gross_margin' as item,
case
when denominator.usd_value != 0 then numerator.usd_value / denominator.usd_value
else null
end as usd_value
from most_recent_staging as denominator
join most_recent_staging as numerator
on denominator.cik = numerator.cik
and denominator.filing_date = numerator.filing_date
and denominator.fiscal_year = numerator.fiscal_year
and denominator.fiscal_quarter = numerator.fiscal_quarter
and denominator.financial_statement = 'income_statement'
and numerator.financial_statement = 'income_statement'
where denominator.item = 'revenues'
and numerator.item = 'gross_profit'

union all

select
denominator.cik,
denominator.filing_date,
'metrics' as financial_statement,
denominator.fiscal_year,
denominator.fiscal_quarter,
'debt_to_equity' as item,
case
when denominator.usd_value != 0 then numerator.usd_value / denominator.usd_value
else null
end as usd_value
from most_recent_staging as denominator
join most_recent_staging as numerator
on denominator.cik = numerator.cik
and denominator.filing_date = numerator.filing_date
and denominator.fiscal_year = numerator.fiscal_year
and denominator.fiscal_quarter = numerator.fiscal_quarter
and denominator.financial_statement = 'balance_sheet'
and numerator.financial_statement = 'balance_sheet'
where denominator.item = 'equity'
and numerator.item = 'liabilities'

union all

select
denominator.cik,
denominator.filing_date,
'metrics' as financial_statement,
denominator.fiscal_year,
denominator.fiscal_quarter,
'current_ratio' as item,
case
when denominator.usd_value != 0 then numerator.usd_value / denominator.usd_value
else null
end as usd_value
from most_recent_staging as denominator
join most_recent_staging as numerator
on denominator.cik = numerator.cik
and denominator.filing_date = numerator.filing_date
and denominator.fiscal_year = numerator.fiscal_year
and denominator.fiscal_quarter = numerator.fiscal_quarter
and denominator.financial_statement = 'balance_sheet'
and numerator.financial_statement = 'balance_sheet'
where denominator.item = 'current_liabilities'
and numerator.item = 'current_assets'

union all

select
denominator.cik,
denominator.filing_date,
'metrics' as financial_statement,
denominator.fiscal_year,
denominator.fiscal_quarter,
'operating_margin' as item,
case
when denominator.usd_value != 0 then numerator.usd_value / denominator.usd_value
else null
end as usd_value
from most_recent_staging as denominator
join most_recent_staging as numerator
on denominator.cik = numerator.cik
and denominator.filing_date = numerator.filing_date
and denominator.fiscal_year = numerator.fiscal_year
and denominator.fiscal_quarter = numerator.fiscal_quarter
and denominator.financial_statement = 'income_statement'
and numerator.financial_statement = 'income_statement'
where denominator.item = 'revenues'
and numerator.item = 'operating_income'

union all

select
denominator.cik,
denominator.filing_date,
'metrics' as financial_statement,
denominator.fiscal_year,
denominator.fiscal_quarter,
'return_on_equity' as item,
case
when denominator.usd_value != 0 then numerator.usd_value / denominator.usd_value
else null
end as usd_value
from most_recent_staging as denominator
join most_recent_staging as numerator
on denominator.cik = numerator.cik
and denominator.filing_date = numerator.filing_date
and denominator.fiscal_year = numerator.fiscal_year
and denominator.fiscal_quarter = numerator.fiscal_quarter
and denominator.financial_statement = 'balance_sheet'
and numerator.financial_statement = 'income_statement'
where denominator.item = 'equity'
and numerator.item = 'net_income'

union all

select
denominator.cik,
denominator.filing_date,
'metrics' as financial_statement,
denominator.fiscal_year,
denominator.fiscal_quarter,
'net_margin' as item,
case
when denominator.usd_value != 0 then numerator.usd_value / denominator.usd_value
else null
end as usd_value
from most_recent_staging as denominator
join most_recent_staging as numerator
on denominator.cik = numerator.cik
and denominator.filing_date = numerator.filing_date
and denominator.fiscal_year = numerator.fiscal_year
and denominator.fiscal_quarter = numerator.fiscal_quarter
and denominator.financial_statement = 'income_statement'
and numerator.financial_statement = 'income_statement'
where denominator.item = 'revenues'
and numerator.item = 'net_income'

union all

select
denominator.cik,
denominator.filing_date,
'metrics' as financial_statement,
denominator.fiscal_year,
denominator.fiscal_quarter,
'return_on_assets' as item,
case
when denominator.usd_value != 0 then numerator.usd_value / denominator.usd_value
else null
end as usd_value
from most_recent_staging as denominator
join most_recent_staging as numerator
on denominator.cik = numerator.cik
and denominator.filing_date = numerator.filing_date
and denominator.fiscal_year = numerator.fiscal_year
and denominator.fiscal_quarter = numerator.fiscal_quarter
and denominator.financial_statement = 'balance_sheet'
and numerator.financial_statement = 'income_statement'
where denominator.item = 'assets'
and numerator.item = 'net_income'
)

select
m.*
from metrics m
where not exists (
    select
    1
    from investment_analytics.staging.staging_financials s
    where s.cik = m.cik
    and s.filing_date = m.filing_date
    and s.financial_statement = m.financial_statement
    and s.fiscal_year = m.fiscal_year
    and s.fiscal_quarter = m.fiscal_quarter
    and s.item = m.item
);


