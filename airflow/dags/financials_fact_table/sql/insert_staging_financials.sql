-- Insert new record into staging table if it doesn't already exist
merge into investment_analytics.staging.staging_financials as target
using (
    select 
    rf.cik,
    rf.filing_date,
    rf.fiscal_year,
    case when rf.fiscal_quarter = 'FY' then 'Q4'
    else rf.fiscal_quarter
    end as fiscal_quarter_normalized,
    rf.fiscal_quarter,
    rf.financial_statement,
    rf.item,
    case when (rf.currency) = 'USD' or (rf.currency) = 'USD/shares' then rf.value
    else rf.value / ucr.local_conversion_rate end as usd_value
    from investment_analytics.raw.raw_financials as rf
    left join investment_analytics.financials.usd_currency_conversion_rates as ucr
    on split_part(rf.currency, '/', 1) = ucr.abbreviation
) as source
on target.cik = source.cik
   and target.fiscal_year = source.fiscal_year
   and target.fiscal_quarter = source.fiscal_quarter_normalized
   and target.item = source.item
   and target.filing_date = source.filing_date

-- Only insert if a given combination of cik, fiscal_year, fiscal_quarter, item, value, currency and filing date does not already exist
when not matched then
insert (
    cik,
    filing_date,
    financial_statement,
    fiscal_year,
    fiscal_quarter,
    item,
    usd_value
)
values (
    source.cik,
    source.filing_date,
    source.financial_statement,
    source.fiscal_year,
    source.fiscal_quarter_normalized,
    source.item,
    source.usd_value
);
