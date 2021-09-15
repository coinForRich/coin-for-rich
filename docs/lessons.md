**What I write here is in the form of Q&A. Much like self-talk.**
# Lessons and Thoughts
## Project Structure
> Why use Docker?

The concept I remember from the OpenDota team is "Fail separately/Scale individually". What the OpenDota team originally meant was not exactly applied in this project, but rather a close idea: by running with Docker, if a service fails, it can be opened back easily.

## Fetching
> Why didn't I used existing libraries like `ccxt` or `coin_feed`?

Because I want to do it from scratch and be able to debug myself.

> Some spontaneous definitions for the next question:

Empty minute: minute with no trading data/OHLCV data
Dummy mimute: minute with 0 volume and OHLC copied from some other minute

> Why I cannot create dummy minutes for empty minutes?

If I were to create dummy minutes, I need data to copy from. For infrequently traded symbols (e.g., DCR-USD on bitfinex), it takes so long for an OHLC candle to appear. Thus, there will be a comparatively long period until I have some data to copy to dummy minutes. Meanwhile, more frequently traded symbols require less time to have some data, which creates an inconsistency among different symbols. It is better not to implement dummy minutes when fetching at all.

> Why use Celery for REST API fetching?

To me, REST API fetching is a long-running task that has a definite time to end. Thus, I want to see all the outputs of the fetching process in a log file (meaning no details are missing - this may be true for a terminal output).

## Charting
> How can I render accurate charts when there are empty minutes?

To render more accurate OHLC charts (in terms of candlesticks' relative distances), empty minutes among non-empty minutes are provided with OHLC averaged from their surrounding non-empty minutes (within the limit number queried) to become dummy minutes. I have not found a better way to create dummy minutes so far.

> How can I keep data on real-time OHLCV chart accurate?

There is an issue of "time lag" when switching back and forth among time periods. Near the end of any minute, if the user clicks another period other than "1h", the latest data bar update for that minute may not be reflected on the chart. I have not found a good way to keep OHLCV chart super-accurate (as when the user switches time periods, the websocket subscription also changes). The only idea I could think of was to have a "reset" chart button to reload chart data.

# Limitations
## Exchanges' REST APIs
The most critical factor affecting the speed of populating the database is the REST API rate limit. There is no way to circumvent this, unless you are on some premium tier that has privileged and elevated access to the APIs.
## Exchanges' Websocket APIs
Disconnections are expected for long-running websocket subscriptions. Thus, data are expected to have gaps. To cover for these gaps, we can create scheduled jobs that fetch actual data from REST APIs of the exchanges. However, this can easily put us well over the exchanges' API rate limits, due to the frequency of the fetching.
