-- If multiple filing dates delete all filings which are not the most recent
delete from investment_analytics.staging.staging_financials
where (cik, fiscal_year, fiscal_quarter, item) not in (
    select cik, fiscal_year, fiscal_quarter, item
    from (
        select *,
               row_number() over (
                   partition by cik, fiscal_year, fiscal_quarter, item
                   order by filing_date desc
               ) as rn
        from investment_analytics.staging.staging_financials
    ) as ranked_by_filing_date
    where rn = 1
);