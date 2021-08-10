import argparse
from celery_app.celery_tasks import *


# Create the parser
arg_parser = argparse.ArgumentParser(
    prog="commands.fetchrest",
    description="Start a REST fetcher for an exchange"
)

# Add the arguments
arg_parser.add_argument(
    'unit',
    metavar='unit',
    type=str,
    choices=["exchange"],
    help='exchange or ..?'
)

arg_parser.add_argument(
    '--exchange',
    metavar='exchange',
    type=str,
    help='name of the exchange'
)

arg_parser.add_argument(
    '--start',
    metavar='start',
    type=str,
    help='Start date; Must comply to this format: %%Y-%%m-%%dT%%H:%%M:%%S',
    required=True
)

arg_parser.add_argument(
    '--end',
    metavar='end',
    type=str,
    help='End date; Must comply to this format: %%Y-%%m-%%dT%%H:%%M:%%S',
    required=True
)

# arg_parser.add_argument(
#     '--symbols'
# )

# Execute the parse_args() method
args = arg_parser.parse_args()
unit = args.unit
exchange = args.exchange
start = args.start
end = args.end
if unit == "exchange":
    if exchange == "bitfinex":
        bitfinex_fetch_ohlcvs_all_symbols.delay(start, end)
    elif exchange == "binance":
        binance_fetch_ohlcvs_all_symbols.delay(start, end)
    elif exchange == "bittrex":
        bittrex_fetch_ohlcvs_all_symbols.delay(start, end)
