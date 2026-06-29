merge into investment_analytics.core.dim_company as target
using (
select 
column1 as company_key,
column2 as cik,
column3 as company_name,
column4 as ticker_symbol,
column5 as industry,
column6 as effective_start,
column7 as effective_end,
column8 as is_current
from values
(-1, 'Not Applicable', 'Non Company', 'Not Applicable', 'Not Applicable',
date('2025-08-08'), date('9999-12-31'), TRUE),
(0,  'Not Applicable', 'Retail Investor', 'Not Applicable', 'Not Applicable',
date('2025-08-08'), date('9999-12-31'), TRUE)
) as source
on target.company_name = source.company_name

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