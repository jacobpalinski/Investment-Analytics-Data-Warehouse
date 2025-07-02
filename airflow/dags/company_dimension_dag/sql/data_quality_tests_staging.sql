-- Clear previous results for this run
DELETE FROM investment_analytics.data_quality.data_quality_results WHERE table_name = 'staging.company_information';

-- NULL check for CIK
INSERT INTO investment_analytics.data_quality.data_quality_results
SELECT
    'Null CIK Check',
    'staging.company_information',
    COUNT(*) AS failed_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    CURRENT_TIMESTAMP
FROM investment_analytics.staging.company_information
WHERE cik IS NULL;

-- Duplicate CIK check
INSERT INTO data_quality_results
SELECT
    'Duplicate CIK Check',
    'staging.company_information',
    COUNT(*) AS failed_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    CURRENT_TIMESTAMP
FROM (
    SELECT cik
    FROM investment_analytics.staging.company_information
    GROUP BY cik
    HAVING COUNT(*) > 1
);

-- NULLs in important columns
INSERT INTO data_quality_results
SELECT
    'Required Columns Null Check',
    'staging.company_information',
    COUNT(*) AS failed_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    CURRENT_TIMESTAMP
FROM investment_analytics.staging.company_information
WHERE company_name IS NULL OR ticker_symbol IS NULL OR industry IS NULL;

-- Raise error if any check fails
BEGIN
    IF EXISTS (
        SELECT 1 FROM data_quality_results
        WHERE table_name = 'staging.company_information' AND status = 'FAIL'
    )
    THEN
        RAISE 'Data quality checks failed for staging.company_information';
    END IF;
END;