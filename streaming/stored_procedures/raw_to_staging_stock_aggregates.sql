CREATE OR REPLACE PROCEDURE INVESTMENT_ANALYTICS.STAGING.RAW_TO_STAGING_STOCK_AGGREGATES()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS 'begin
merge into investment_analytics.staging.staging_stock_aggregates as target
using (
select
record_content:"ticker_symbol"::string as ticker_symbol,
to_timestamp_ntz(record_content:"event_timestamp"::number / 1000) as event_timestamp,
record_content:"volume"::number as volume,
record_content:"accumulated_volume"::number as accumulated_volume,
record_content:"volume_weighted_average_price"::float as volume_weighted_average_price,
record_content:"closing_price"::float as closing_price,
record_content:"average_trade_size"::float as average_trade_size
from investment_analytics.staging.staging_stock_aggregates_stream
where to_timestamp_ntz(record_content:"event_timestamp"::number / 1000)
>= current_timestamp() - interval ''1 DAY'') as source
on source.event_timestamp = target.event_timestamp
and source.ticker_symbol = target.ticker_symbol

when not matched then
insert (
ticker_symbol,
event_timestamp,
volume,
accumulated_volume,
volume_weighted_average_price,
closing_price,
average_trade_size
)
values (
source.ticker_symbol,
source.event_timestamp,
source.volume,
source.accumulated_volume,
source.volume_weighted_average_price,
source.closing_price,
source.average_trade_size
);
return ''raw_to_staging_stock_aggregates successfully completed'';
end';