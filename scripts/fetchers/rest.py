import argparse
from celery_app.celery_tasks import *


# Create the parser
arg_parser = argparse.ArgumentParser(
    prog="python -m scripts.fetchers.rest",
    description="Starts a REST fetcher for an exchange using Celery"
)

# Add the arguments
arg_parser.add_argument(
    'action',
    metavar='action',
    type=str,
    choices=["fetch", "resume"],
    help='fetch or resume'
)

arg_parser.add_argument(
    '--exchange',
    metavar='exchange',
    type=str,
    required=True,
    help='name of the exchange'
)

arg_parser.add_argument(
    '--start',
    metavar='start',
    type=str,
    help='Start date; Must comply to this format: %%Y-%%m-%%dT%%H:%%M:%%S; \nMust be entered if action is fetch',
    # required=True,
)

arg_parser.add_argument(
    '--end',
    metavar='end',
    type=str,
    help='End date; Must comply to this format: %%Y-%%m-%%dT%%H:%%M:%%S; \nMust be entered if action is fetch',
    # required=True,
)

# Execute the parse_args() method
args = arg_parser.parse_args()
action = args.action
exchange = args.exchange
start = args.start
end = args.end
if action == "fetch":
    if exchange == "bitfinex":
        bitfinex_fetch_ohlcvs_all_symbols.delay(start, end)
    elif exchange == "binance":
        binance_fetch_ohlcvs_all_symbols.delay(start, end)
    elif exchange == "bittrex":
        bittrex_fetch_ohlcvs_all_symbols.delay(start, end)
elif action == "resume":
    if exchange == "bitfinex":
        bitfinex_resume_fetch.delay()
    elif exchange == "binance":
        binance_resume_fetch.delay()
    elif exchange == "bittrex":
        bittrex_resume_fetch.delay()
