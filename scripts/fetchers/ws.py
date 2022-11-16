import argparse
from fetchers.ws.bitfinex import BitfinexOHLCVWebsocket
from fetchers.ws.binance import BinanceOHLCVWebsocket
from fetchers.ws.bittrex import BittrexOHLCVWebsocket
from fetchers.ws.updater import OHLCVWebsocketUpdater


# Create the parser
arg_parser = argparse.ArgumentParser(
    prog="python -m scripts.fetchers.ws",
    description="Starts a websocket fetcher for an exchange or an updater"
)

# Add the arguments
arg_parser.add_argument(
    'action',
    metavar='action',
    type=str,
    choices=["fetch", "update"],
    help='fetch (for an exchange) or update (collect fetched data to db)'
)

arg_parser.add_argument(
    '--exchange',
    metavar='exchange',
    type=str,
    help='name of the exchange; Only needed if action is fetch'
)

arg_parser.add_argument(
    '--log_filename',
    metavar='log_filename',
    type=str,
    help='full path to the log filename'
)

# Execute the parse_args() method
args = arg_parser.parse_args()
action = args.action
exchange = args.exchange
log_filename = args.log_filename
if action == "fetch":
    if exchange == "bitfinex":
        ws = BitfinexOHLCVWebsocket(log_filename=log_filename)
    elif exchange == "binance":
        ws = BinanceOHLCVWebsocket(log_filename=log_filename)
    elif exchange == "bittrex":
        ws = BittrexOHLCVWebsocket(log_filename=log_filename)
    ws.run_all()
elif action == "update":
    ws = OHLCVWebsocketUpdater(log_filename=log_filename)
    ws.update()
