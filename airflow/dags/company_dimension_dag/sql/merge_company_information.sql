merge into investment_analytics.staging.company_information as target
using (
    select
        cik,
        company_name,
        ticker_symbol,
        industry
        from investment_analytics.raw.raw_company_information
) as source
on target.cik = source.cik

when matched then
update set
    target.company_name = source.company_name,
    target.ticker_symbol = source.ticker_symbol,
    target.industry = source.industry

when not matched then
insert (cik, company_name, ticker_symbol, industry) 
values (source.cik, source.company_name, source.ticker_symbol, source.industry);