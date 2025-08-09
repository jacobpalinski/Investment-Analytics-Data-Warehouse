insert into investment_analytics.financials.fact_financials (
financials_fact_key,
period_key, 
company_key,
financial_statement,
filing_date,
item,
currency,
value
)
select
investment_analytics.financials.financials_fact_key_seq.nextval,
p.period_key,
c.company_key,
s.financial_statement,
s.filing_date,
s.item,
s.currency,
s.value
from investment_analytics.staging.staging_financials s
inner join investment_analytics.financials.dim_period p
on s.fiscal_year = p.fiscal_year
and s.fiscal_quarter = p.fiscal_quarter
inner join investment_analytics.core.dim_company c
on s.cik = c.cik
where not exists (
    select 1
    from investment_analytics.financials.fact_financials f
    where f.period_key = p.period_key
    and f.company_key = c.company_key
    and f.financial_statement = s.financial_statement
    and f.filing_date = s.filing_date
    and f.item = s.item
    and f.currency = s.currency
    and f.value = s.value
);




