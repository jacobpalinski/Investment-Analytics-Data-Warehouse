-- Insert new record into fact table provided it doesn't already exist
merge into investment_analytics.sentiment.fact_sentiments f
using (
    select 
    distinct
    dim_sd.sentiment_date_key,
    dim_s.source_key,
    c.company_key,
    s.sentiment_category,
    s.article_sentiment_source,
    s.sentiment_score
    from investment_analytics.staging.staging_combined_with_sentiment s
    join investment_analytics.sentiment.dim_sentiment_date dim_sd
      on s.date = dim_sd.date
    join investment_analytics.sentiment.dim_source dim_s
      on s.source = dim_s.source_name
    join investment_analytics.core.dim_company c
      on s.ticker_symbol = c.ticker_symbol
    where c.is_current = true
    ) src
on f.sentiment_date_key = src.sentiment_date_key
and f.source_key = src.source_key
and f.company_key = src.company_key
and f.sentiment_category = src.sentiment_category
and f.article_sentiment_source = src.article_sentiment_source
and f.sentiment_score = src.sentiment_score
when not matched then
  insert (
    sentiments_fact_key,
    sentiment_date_key,
    source_key,
    company_key,
    sentiment_category,
    article_sentiment_source,
    sentiment_score
  )
  values (
    investment_analytics.sentiment.sentiments_fact_key_seq.nextval,
    src.sentiment_date_key,
    src.source_key,
    src.company_key,
    src.sentiment_category,
    src.article_sentiment_source,
    src.sentiment_score
  );