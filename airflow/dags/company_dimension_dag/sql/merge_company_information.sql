-- Merge raw table records into staging table provided record doesn't yet exist
merge into investment_analytics.staging.staging_company_information as target
using (
select
*
from (
select
extraction_timestamp,
cik,
company_name,
ticker_symbol,
industry,
row_number() over (partition by cik order by length(ticker_symbol) asc, ticker_symbol asc) as rn
from investment_analytics.raw.raw_company_information
) as with_rn
where rn = 1
and industry not in (null, 'N/A', '')
and company_name not in (null, 'N/A', '')
and cik not in (null, 'N/A', '')
) as source
on target.cik = source.cik

when matched then
update set
target.company_name = source.company_name,
target.ticker_symbol = source.ticker_symbol,
target.industry = source.industry

when not matched then
insert (cik, company_name, ticker_symbol, industry) 
values (source.extraction_timestamp, source.cik, source.company_name, source.ticker_symbol, source.industry);