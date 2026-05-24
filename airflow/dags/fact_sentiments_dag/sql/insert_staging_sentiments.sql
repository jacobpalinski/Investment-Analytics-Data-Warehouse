-- Insert new record into staging table
merge into investment_analytics.staging.staging_non_company_news as target
using (
select 
* 
from investment_analytics.raw.raw_non_company_news
) as source
on target.title = source.title
and target.description = source.description

-- Only insert if a given combination of date, title, description and source does not exist
when not matched then
insert (
    date,
    title,
    description,
    source
)
values (
    source.date,
    source.title,
    source.description,
    source.source
);