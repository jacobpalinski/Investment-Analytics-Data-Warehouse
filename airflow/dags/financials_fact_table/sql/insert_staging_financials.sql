merge into investment_analytics.staging.staging_financials as target
using (
    select 
    *
    from investment_analytics.raw.raw_financials
) as source
on target.cik = source.cik
   and target.fiscal_year = source.fiscal_year
   and target.fiscal_quarter = source.fiscal_quarter
   and target.item = source.item
   and target.currency = source.currency
   and target.filing_date = source.filing_date

-- Only insert if a given combination of cik, fiscal_year, fiscal_quarter, item, value, currency and filing date does not already exist
when not matched then
insert (
    cik,
    currency,
    filing_date,
    financial_statement,
    fiscal_year,
    fiscal_quarter,
    item,
    value
)
values (
    source.cik,
    source.currency,
    source.filing_date,
    source.financial_statement,
    source.fiscal_year,
    source.fiscal_quarter,
    source.item,
    source.value
);
