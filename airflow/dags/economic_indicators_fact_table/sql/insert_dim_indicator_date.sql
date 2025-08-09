-- Insert new year, quarter, month, and day into the dimension table
insert into investment_analytics.economic_indicators.dim_indicator_date (
indicator_date_key,
year,
quarter,
month,
day
)
select
investment_analytics.economic_indicators.indicator_date_key_seq.nextval,
s.year,
s.quarter,
s.month,
s.day
from investment_analytics.staging.staging_economic_indicators s
where not exists (
    select 1
    from investment_analytics.economic_indicators.dim_indicator_date d
    where d.year = s.year
      and d.quarter = s.quarter
      and d.month = s.month
      and d.day = s.day
);
