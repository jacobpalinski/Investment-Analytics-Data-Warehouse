insert into investment_analytics.analytics.dim_economic_indicators (
economic_indicators_key,
year,
quarter,
month,
interest_rate,
consumer_price_index,
unemployment_rate,
gdp_growth_rate,
consumer_confidence,
effective_start,
effective_end,
is_current
)
select
investment_analytics.analytics.economic_indicators_key_seq.nextval as economic_indicator_key,
s.year,
s.quarter,
s.month,
s.interest_rate,
s.consumer_price_index,
s.unemployment_rate,
s.gdp_growth_rate,
s.consumer_confidence,
current_date as effective_start,
null as effective_end,
true as is_current
from investment_analytics.staging.staging_economic_indicators s
where not exists (
    select 1
    from investment_analytics.analytics.dim_economic_indicators d
    where d.year = s.year
      and d.quarter = s.quarter
      and d.month = s.month
      and d.interest_rate = s.interest_rate
      and d.consumer_price_index = s.consumer_price_index
      and d.unemployment_rate = s.unemployment_rate
      and d.gdp_growth_rate = s.gdp_growth_rate
      and d.consumer_confidence = s.consumer_confidence
);