merge into investment_analytics.staging.staging_economic_indicators as target
using (
    with pivoted_raw_economic_indicators as (
    select
    year,
    quarter,
    month,
    max(case when indicator = 'interest_rate' then value end) as interest_rate,
    max(case when indicator = 'consumer_price_index' then value end) as consumer_price_index,
    max(case when indicator = 'unemployment_rate' then value end) as unemployment_rate,
    max(case when indicator = 'gdp_growth_rate' then value end) as gdp_growth_rate,
    max(case when indicator = 'consumer_confidence' then value end) as consumer_confidence
    from investment_analytics.raw.raw_economic_indicators
    group by year, quarter, month
)
    select * from pivoted_raw_economic_indicators
) as source
on target.year = source.year
   and target.quarter = source.quarter
   and target.month = source.month
   and target.interest_rate = source.interest_rate
   and target.consumer_price_index = source.consumer_price_index
   and target.unemployment_rate = source.unemployment_rate
   and target.gdp_growth_rate = source.gdp_growth_rate
   and target.consumer_confidence = source.consumer_confidence

-- Only insert if a given combination of year, quarter, month, interest_rate, consumer_price_index, unemployment_rate, gdp_growth_rate and consumer_confidence does not already exist
when not matched then
insert (
    year,
    quarter,
    month,
    interest_rate,
    consumer_price_index,
    unemployment_rate,
    gdp_growth_rate,
    consumer_confidence
)
values (
    source.year,
    source.quarter,
    source.month,
    source.interest_rate,
    source.consumer_price_index,
    source.unemployment_rate,
    source.gdp_growth_rate,
    source.consumer_confidence
);