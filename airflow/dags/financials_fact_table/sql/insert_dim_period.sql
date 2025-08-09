insert into investment_analytics.financials.dim_period (
period_key,
fiscal_year,
fiscal_quarter,
)
select
investment_analytics.financials.period_key_seq.nextval,
s.fiscal_year,
s.fiscal_quarter
from investment_analytics.staging.staging_financials s
left join investment_analytics.financials.dim_period d
on s.fiscal_year = d.fiscal_year
and s.fiscal_quarter = d.fiscal_quarter
where d.fiscal_year is null
or (d.fiscal_year != s.fiscal_year
or d.fiscal_quarter != s.fiscal_quarter);
