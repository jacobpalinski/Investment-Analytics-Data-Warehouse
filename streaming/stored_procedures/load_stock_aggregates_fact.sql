CREATE OR REPLACE PROCEDURE INVESTMENT_ANALYTICS.STAGING.LOAD_STOCK_AGGREGATES_FACT()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS 'begin
insert into investment_analytics.staging.staging_stock_aggregates_quarantine (ticker_symbol, event_timestamp, volume, accumulated_volume, volume_weighted_average_price,
closing_price, average_trade_size)
select
ticker_symbol,
event_timestamp,
volume,
accumulated_volume,
volume_weighted_average_price,
closing_price,
average_trade_size
from investment_analytics.staging.staging_stock_aggregates
where ticker_symbol is null
or event_timestamp is null
or volume is null
or accumulated_volume is null
or volume_weighted_average_price is null
or closing_price is null
or average_trade_size is null
or volume < 0
or accumulated_volume < 0
or volume_weighted_average_price < 0
or closing_price < 0
or average_trade_size < 0;

delete from investment_analytics.staging.staging_stock_aggregates
where ticker_symbol is null
or event_timestamp is null
or volume is null
or accumulated_volume is null
or volume_weighted_average_price is null
or closing_price is null
or average_trade_size is null
or volume < 0
or accumulated_volume < 0
or volume_weighted_average_price < 0
or closing_price < 0
or average_trade_size < 0;

merge into investment_analytics.stocks.dim_date d
using (
select
distinct
cast(event_timestamp as date) as date,
extract(year from event_timestamp) as year,
extract(month from event_timestamp) as month,
extract(quarter from event_timestamp) as quarter,
extract(day from event_timestamp) as day,
to_varchar(event_timestamp, ''Day'') as day_of_week,
extract(hour from event_timestamp) as hour,
extract(minute from event_timestamp) as minute
from investment_analytics.staging.staging_stock_aggregates
where event_timestamp >= dateadd(day, -1, current_timestamp())
and event_timestamp < current_timestamp()
) as s
on s.date = d.date
and to_varchar(s.hour) = d.hour
and to_varchar(s.minute) = d.minute
when not matched then
insert (
date_key,
date,
year,
month,
quarter,
day,
day_of_week,
hour,
minute
)
values (
investment_analytics.stocks.date_key_seq.nextval,
s.date,
s.year,
s.month,
s.quarter,
s.day,
s.day_of_week,
s.hour,
s.minute
);

merge into investment_analytics.stocks.fact_stock_aggregates f
using (
select
d.date_key,
c.company_key,
s.event_timestamp,
s.ticker_symbol,
s.volume,
s.accumulated_volume,
s.volume_weighted_average_price,
s.closing_price,
s.average_trade_size
from investment_analytics.staging.staging_stock_aggregates as s
join investment_analytics.stocks.dim_date as d
on d.date = cast(s.event_timestamp as date)
and d.hour = extract(hour from s.event_timestamp)
and d.minute = extract(minute from s.event_timestamp)
join investment_analytics.core.dim_company c
on c.ticker_symbol = s.ticker_symbol
where c.is_current = true
and s.event_timestamp >= dateadd(day, -1, current_timestamp())
and s.event_timestamp < current_timestamp()
) as src
on f.date_key = src.date_key
and f.company_key = src.company_key
and f.accumulated_volume = src.accumulated_volume
when not matched then
insert (
stock_aggregates_fact_key,
date_key,
company_key,
volume,
accumulated_volume,
volume_weighted_average_price,
closing_price,
average_trade_size
)
values (
investment_analytics.stocks.stock_aggregates_fact_key_seq.nextval, -- surrogate key sequence
src.date_key,
src.company_key,
src.volume,
src.accumulated_volume,
src.volume_weighted_average_price,
src.closing_price,
src.average_trade_size
);
return ''load_stock_aggregates successfully completed'';
end';