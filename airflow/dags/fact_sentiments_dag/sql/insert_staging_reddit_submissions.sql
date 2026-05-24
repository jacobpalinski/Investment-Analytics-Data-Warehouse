-- Filter for titles not in staging table and convert utc time to date before inserting into staging table
merge into investment_analytics.staging.staging_reddit_submissions as target
using (
select
distinct
to_date(convert_timezone('UTC', 'America/New_York', to_timestamp_ntz(cast(r.date as bigint)))) as date,
title,
description,
source
from investment_analytics.raw.raw_reddit_submissions r
) as source
on target.title = source.title
and target.description = source.description
when not matched then
    insert (
        date,
        title,
        description,
        source
    )
    values (
        source.date,
        source.title,
        source.description,
        source.source
    );
