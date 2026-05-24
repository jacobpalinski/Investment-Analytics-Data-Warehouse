-- Clear previous results for this run
delete from investment_analytics.data_quality.data_quality_results where table_name = 'STAGING.STAGING_COMPANY_INFORMATION';

-- Duplicate CIK check
insert into investment_analytics.data_quality.data_quality_results
select
'Duplicate CIK Check',
'STAGING.STAGING_COMPANY_INFORMATION',
count(*) as failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from (
select cik
from investment_analytics.staging.staging_company_information
group by cik
having count(*) > 1
);

-- Nulls in important columns
insert into investment_analytics.data_quality.data_quality_results
select
'Required Columns Null Check',
'STAGING.STAGING_COMPANY_INFORMATION',
count(*) as failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from investment_analytics.staging.staging_company_information
where extraction_timestamp is null or cik is null or company_name is null or ticker_symbol is null or industry is null;

-- Invalid values (non null) in import columns
insert into investment_analytics.data_quality.data_quality_results
select
'Invalid Column Values Check',
'STAGING.STAGING_COMPANY_INFORMATION',
count(*) as failed_count,
case when count(*) = 0 then 'PASS' else 'FAIL' end,
current_timestamp
from investment_analytics.staging.staging_company_information
where industry in ('N/A', '')
and company_name in ('N/A', '')
and cik in ('N/A', '')
and ticker_symbol in ('N/A', '')
and extraction_timestamp in ('N/A', '');