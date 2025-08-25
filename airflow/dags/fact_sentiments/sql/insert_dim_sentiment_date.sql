merge into investment_analytics.sentiment.dim_sentiment_date d
using (
    select 
    distinct
    s.date,
    to_char(s.date, 'DAY') as day_of_week,
    month(s.date) as month,
    quarter(s.date) as quarter,
    year(s.date) as year,
    day(s.date) as day
    from investment_analytics.staging.staging_combined_with_sentiment s
) src
on d.date = src.date
when not matched then
  insert (
    sentiment_date_key,
    date,
    year,
    quarter,
    month,
    day,
    day_of_week
  )
  values (
    investment_analytics.sentiment.sentiment_date_key_seq.nextval,
    src.date,
    src.year,
    src.quarter,
    src.month,
    src.day,
    src.day_of_week
  );
