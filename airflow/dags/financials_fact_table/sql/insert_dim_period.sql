-- Insert new record into dimension table if it doesn't already exist
merge into investment_analytics.financials.dim_period d
using (
    select distinct fiscal_year, fiscal_quarter
    from investment_analytics.staging.staging_financials
) s
on d.fiscal_year = s.fiscal_year
and d.fiscal_quarter = s.fiscal_quarter
when not matched then
  insert (period_key, fiscal_year, fiscal_quarter)
  values (investment_analytics.financials.period_key_seq.nextval, s.fiscal_year, s.fiscal_quarter);
