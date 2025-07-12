update investment_analytics.analytics.dim_company d
set is_current = false,
effective_end = current_date
from investment_analytics.staging.staging_company_information s
where d.cik = s.cik
and d.is_current = true
and (
d.company_name != s.company_name
or d.ticker_symbol != s.ticker_symbol
or d.industry != s.industry
);