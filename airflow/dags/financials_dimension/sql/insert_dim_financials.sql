insert into investment_analytics.analytics.dim_financials (
financials_key, 
cik,
fiscal_year,
fiscal_quarter,
financial_statement,
item,
currency,
value
)
select
investment_analytics.analytics.financials_key_seq.nextval as financials_key,
s.cik,
s.fiscal_year,
s.fiscal_quarter,
s.financial_statement,
s.item,
s.currency,
s.value
from investment_analytics.staging.staging_financials s
left join investment_analytics.analytics.dim_financials d
on s.cik = d.cik
and s.fiscal_year = d.fiscal_year
and s.fiscal_quarter = d.fiscal_quarter
and s.item = d.item
where d.cik is null
or (d.fiscal_year != s.fiscal_year
or d.fiscal_quarter != s.fiscal_quarter
or d.item != s.item
or d.currency != s.currency
or d.value != s.value);


