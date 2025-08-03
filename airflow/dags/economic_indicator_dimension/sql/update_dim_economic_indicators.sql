update investment_analytics.analytics.dim_economic_indicators as d
set is_current = false,
effective_end = current_date
from investment_analytics.staging.staging_economic_indicators as s
where d.year = s.year
and d.quarter = s.quarter
and d.month = s.month
and d.is_current = true
and (
d.interest_rate != s.interest_rate
or d.consumer_price_index != s.consumer_price_index
or d.unemployment_rate != s.unemployment_rate
or d.gdp_growth_rate != s.gdp_growth_rate
or d.consumer_confidence != s.consumer_confidence
);