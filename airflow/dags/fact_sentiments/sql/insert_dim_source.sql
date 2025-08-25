merge into investment_analytics.sentiment.dim_source d
using (
    select 
    distinct source
    from investment_analytics.staging.staging_combined_with_sentiment
) src
on d.source_name = src.source
when not matched then
insert (
    source_key,
    source_name
  )
  values (
    investment_analytics.sentiment.source_key_seq.nextval,
    src.source
  );