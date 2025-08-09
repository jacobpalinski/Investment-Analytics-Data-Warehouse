insert into investment_analytics.economic_indicators.fact_economic_indicators (
economic_indicators_fact_key,
indicator_date_key,
indicator,
value
)
select
investment_analytics.economic_indicators.economic_indicators_fact_key_seq.nextval,
d.indicator_date_key,
s.indicator,
s.value
from investment_analytics.staging.staging_economic_indicators s
join investment_analytics.economic_indicators.dim_indicator_date d
on s.year = d.year
and s.quarter = d.quarter
and s.month = d.month
and s.day = d.day
where not exists (
    select 1
    from investment_analytics.economic_indicators.fact_economic_indicators f
    where f.indicator_date_key = d.indicator_date_key
      and f.indicator = s.indicator
      and f.value = s.value
);