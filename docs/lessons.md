# Lessons
Fetching
Definitions:
Empty minute: minute with no trading data/OHLCV data
Dummy mimute: minute with 0 volume and OHLC copied from some other minute

Why I cannot create dummy minutes for empty minutes?
If I were to create dummy minutes, I need data to copy from. For infrequently traded symbols (e.g., DCR-USD on bitfinex), it takes so long for an OHLC candle to appear. Thus, there will be a comparatively long period until I have some data to copy to dummy minutes. Meanwhile, more frequently traded symbols require less time to have some data, which creates an inconsistency among different symbols. It is better not to implement dummy minutes when fetching at all.

## Charting
How can I render accurate charts when there are empty minutes?
To render more accurate OHLC charts (in terms of candlesticks' relative distances), empty minutes among non-empty minutes are provided with OHLC averaged from their surrounding non-empty minutes (within the limit number queried) to become dummy minutes. I have not found a better way to create dummy minutes so far.

How can I keep data on real-time OHLCV chart accurate?
There is an issue of "time lag" when switching back and forth among time periods. Near the end of any minute, if the user clicks another period other than "1h", the latest data bar update for that minute may not be reflected on the chart.

# Limitations
Websockets
Disconnections are expected. Thus, data are expected to have gaps
