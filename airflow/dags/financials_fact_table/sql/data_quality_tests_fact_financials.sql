-- Clear previous results for dimension table checks
delete from investment_analytics.data_quality.data_quality_results where table_name = 'FINANCIALS.FACT_FINANCIALS';

-- Check surrogate key is unique
insert into investment_analytics.data_quality.data_quality_results
select
'Duplicate financials_fact_key',
'FINANCIALS.FACT_FINANCIALS',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from (
select financials_fact_key
from investment_analytics.financials.fact_financials
group by financials_fact_key
having count(*) > 1
);

-- Check there are no null values for period_key and company_key
insert into investment_analytics.data_quality.data_quality_results
select
'Null Surrogate Keys',
'FINANCIALS.FACT_FINANCIALS',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from
(select
period_key,
company_key
from investment_analytics.financials.fact_financials
where period_key is null
or company_key is null);

-- Check there are no duplicate period_key, company_key, financial_statement, filing_date, item and usd_value combinations
insert into investment_analytics.data_quality.data_quality_results
select
'Duplicate Financial Entries',
'FINANCIALS.FACT_FINANCIALS',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from
(select
period_key,
company_key,
financial_statement,
filing_date,
item,
usd_value
from investment_analytics.financials.fact_financials
group by period_key, company_key, financial_statement, filing_date, item, usd_value
having count(*) > 1
)


