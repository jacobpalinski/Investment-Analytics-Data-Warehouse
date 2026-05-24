-- Insert new economic indicators into the staging table
merge into investment_analytics.staging.staging_economic_indicators as target
using (
    select * 
    from investment_analytics.raw.raw_economic_indicators
) as source
on target.date = source.date
and target.indicator = source.indicator
and target.value = source.value

-- Only insert if a record does not already exist
when not matched then
insert (
    date,
    year,
    quarter,
    month,
    day,
    indicator,
    value
)
values (
    source.date,
    source.year,
    source.quarter,
    source.month,
    source.day,
    source.indicator,
    source.value
);