merge into investment_analytics.staging.staging_company_news as target
using (
    select 
    distinct
    * 
    from investment_analytics.raw.raw_company_news
) as source
on target.title = source.title
and target.description = source.description
and target.ticker_symbol = source.ticker_symbol

-- Only insert if a given combination of date, title, description and source does not exist
when not matched then
insert (
    date,
    ticker_symbol,
    title,
    description,
    source
)
values (
    source.date,
    source.ticker_symbol,
    source.title,
    source.description,
    source.source
);

