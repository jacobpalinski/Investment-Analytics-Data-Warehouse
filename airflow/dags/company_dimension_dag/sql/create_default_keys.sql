merge into investment_analytics.core.dim_company as target
using (
select 
*
from values
(-1, 'Not Applicable', 'Non Company', 'Not Applicable', 'Not Applicable',
date('2025-08-08'), date('9999-12-31'), TRUE),
(0,  'Not Applicable', 'Retail Investor', 'Not Applicable', 'Not Applicable',
date('2025-08-08'), date('9999-12-31'), TRUE)
as source (
company_key,
cik,
company_name,
ticker_symbol,
industry,
effective_start,
effective_end,
is_current
))
on target.company_id = source.company_id

when not matched then
insert (
company_key,
cik,
company_name,
ticker_symbol,
industry,
effective_start,
effective_end,
is_current
)
values (
source.company_key,
source.cik,
source.company_name,
source.ticker_symbol,
source.industry,
source.effective_start,
source.effective_end,
source.is_current
);