-- Combine individual staging tables into a single table without sentiment scores
merge into investment_analytics.staging.staging_combined_no_sentiment as target
using (
select
date,
'Company' as sentiment_category,
ticker_symbol,
title,
description,
source
from investment_analytics.staging.staging_company_news
union
select
date,
'Non Company' as sentiment_category,
'Not Applicable' as ticker_symbol,
title,
description,
source
from investment_analytics.staging.staging_non_company_news
union
select
date,
'Retail Investor' as sentiment_category,
'Not Applicable' as ticker_symbol,
title,
description,
source
from investment_analytics.staging.staging_reddit_submissions
) as source_data
on target.date = source_data.date
and target.sentiment_category = source_data.sentiment_category
and target.ticker_symbol = source_data.ticker_symbol
and target.title = source_data.title
and target.description = source_data.description
and target.source = source_data.source

when not matched then
    insert (
        date,
        sentiment_category,
        ticker_symbol,
        title,
        description,
        source
    )
    values (
        source_data.date,
        source_data.sentiment_category,
        source_data.ticker_symbol,
        source_data.title,
        source_data.description,
        source_data.source
    );
