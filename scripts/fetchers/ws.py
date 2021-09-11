import argparse
from fetchers.ws.bitfinex import BitfinexOHLCVWebsocket
from fetchers.ws.binance import BinanceOHLCVWebsocket
from fetchers.ws.bittrex import BittrexOHLCVWebsocket
from fetchers.ws.updater import OHLCVWebsocketUpdater


# Create the parser
arg_parser = argparse.ArgumentParser(
    prog="commands.fetchws",
    description="Starts a websocket fetcher for an exchange or an updater"
)

# Add the arguments
arg_parser.add_argument(
    'action',
    metavar='action',
    type=str,
    choices=["fetch", "update"],
    help='fetcher (for an exchange) or updater (collect fetched data to db)'
)

arg_parser.add_argument(
    '--exchange',
    metavar='exchange',
    type=str,
    help='name of the exchange; Only needed if action is fetch'
)

# Execute the parse_args() method
args = arg_parser.parse_args()
action = args.action
exchange = args.exchange
if action == "fetch":
    if exchange == "bitfinex":
        ws = BitfinexOHLCVWebsocket()
    elif exchange == "binance":
        ws = BinanceOHLCVWebsocket()
    elif exchange == "bittrex":
        ws = BittrexOHLCVWebsocket()
    ws.run_all()
elif action == "update":
    ws = OHLCVWebsocketUpdater()
    ws.update()
