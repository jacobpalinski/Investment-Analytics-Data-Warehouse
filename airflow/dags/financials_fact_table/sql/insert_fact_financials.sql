merge into investment_analytics.financials.fact_financials f
using (
    select distinct
        p.period_key,
        c.company_key,
        s.financial_statement,
        s.filing_date,
        s.item,
        s.currency,
        s.value
    from investment_analytics.staging.staging_financials s
    join investment_analytics.financials.dim_period p
      on s.fiscal_year = p.fiscal_year
     and s.fiscal_quarter = p.fiscal_quarter
    join investment_analytics.core.dim_company c
      on s.cik = c.cik
    where c.is_current = true
) t
on f.period_key = t.period_key
and f.company_key = t.company_key
and f.financial_statement = t.financial_statement
and f.filing_date = t.filing_date
and f.item = t.item
and f.currency = t.currency

when not matched then
  insert (
      financials_fact_key,
      period_key,
      company_key,
      financial_statement,
      filing_date,
      item,
      currency,
      value
  )
  values (
      investment_analytics.financials.financials_fact_key_seq.nextval,
      t.period_key,
      t.company_key,
      t.financial_statement,
      t.filing_date,
      t.item,
      t.currency,
      t.value
  );




