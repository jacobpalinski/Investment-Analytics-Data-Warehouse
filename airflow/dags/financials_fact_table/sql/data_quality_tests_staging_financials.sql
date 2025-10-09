-- Clear previous results for dimension table checks
delete from investment_analytics.data_quality.data_quality_results where table_name = 'STAGING.STAGING_FINANCIALS';

-- Check for nulls in key fields
insert into investment_analytics.data_quality.data_quality_results
select
'Null rows',
'STAGING.STAGING_FINANCIALS',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from 
(select
*
from investment_analytics.staging.staging_financials
where cik is null
   or filing_date is null
   or fiscal_year is null 
   or fiscal_quarter is null 
   or financial_statement is null 
   or item is null
   or usd_value is null);

-- Check for duplicate rows
insert into investment_analytics.data_quality.data_quality_results
select
'Duplicate rows',
'STAGING.STAGING_FINANCIALS',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from 
(select cik, filing_date, fiscal_year, fiscal_quarter, financial_statement, item, usd_value
from investment_analytics.staging.staging_financials
group by cik, filing_date, fiscal_year, fiscal_quarter, financial_statement, item, usd_value
having count(*) > 1);

-- Check for outliers (small and large)
insert into investment_analytics.data_quality.data_quality_results
select
'Outliers',
'STAGING.STAGING_FINANCIALS',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from 
(select
*
from investment_analytics.staging.staging_financials
where usd_value > 1e12 or usd_value < -1e12);

-- Check for any unexpected negative values
insert into investment_analytics.data_quality.data_quality_results
select
'Unexpected negative values',
'STAGING.STAGING_FINANCIALS',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from 
(select
*
from investment_analytics.staging.staging_financials
where item in ('revenues', 'assets', 'liabilities', 'current_assets', 'current_liabilities', 'noncurrent_liabilities')
and usd_value < 0);

-- Check for invalid fiscal_year or fiscal_quarter
insert into investment_analytics.data_quality.data_quality_results
select
'Invalid fiscal_year or fiscal_quarter',
'STAGING.STAGING_FINANCIALS',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from 
(select
*
from investment_analytics.staging.staging_financials
where fiscal_year > extract(year from current_date) + 1
or fiscal_quarter not in ('Q1', 'Q2', 'Q3', 'FY'));

-- Check for invalid filing_date
insert into investment_analytics.data_quality.data_quality_results
select
'Invalid filing_date',
'STAGING.STAGING_FINANCIALS',
count(*) AS failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from
(select
*
from investment_analytics.staging.staging_financials
where filing_date > current_date);

