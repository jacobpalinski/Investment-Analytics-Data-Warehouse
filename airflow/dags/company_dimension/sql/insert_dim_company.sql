insert into investment_analytics.core.dim_company (
company_key, 
cik,
company_name,
ticker_symbol,
industry,
effective_start,
effective_end,
is_current
)
select
investment_analytics.core.company_key_seq.nextval as company_key,
s.cik,
s.company_name,
s.ticker_symbol,
s.industry,
current_date as effective_start,
'9999-12-31' as effective_end,
true as is_current
from investment_analytics.staging.staging_company_information s
left join investment_analytics.core.dim_company d
on s.cik = d.cik and d.is_current = TRUE
where d.cik is null
or (
d.company_name != s.company_name
or d.ticker_symbol != s.ticker_symbol
or d.industry != s.industry
);
