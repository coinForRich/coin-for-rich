import argparse
from fetchers.ws.bitfinex import BitfinexOHLCVWebsocket
from fetchers.ws.binance import BinanceOHLCVWebsocket
from fetchers.ws.bittrex import BittrexOHLCVWebsocket
from fetchers.ws.updater import OHLCVWebsocketUpdater


# Create the parser
arg_parser = argparse.ArgumentParser(
    prog="commands.fetchws",
    description="Start a websocket fetcher for an exchange"
)

# Add the arguments
arg_parser.add_argument(
    'unit',
    metavar='unit',
    type=str,
    choices=["exchange", "updater"],
    help='exchange or updater'
)

arg_parser.add_argument(
    '-E',
    metavar='exchange',
    type=str,
    help='name of the exchange'
)

# Execute the parse_args() method
args = arg_parser.parse_args()
unit = args.unit
exchange = args.exchange
if unit == "exchange":
    if exchange == "bitfinex":
        ws = BitfinexOHLCVWebsocket()
    elif exchange == "binance":
        ws = BinanceOHLCVWebsocket()
    elif exchange == "bittrex":
        ws = BittrexOHLCVWebsocket()
    ws.run_all()
elif unit == "updater":
    ws = OHLCVWebsocketUpdater()
    ws.update()
