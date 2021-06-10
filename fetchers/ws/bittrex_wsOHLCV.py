#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Last tested 2020/09/24 on Python 3.8.5
#
# Note: This file is intended solely for testing purposes and may only be used
#   as an example to debug and compare with your code. The 3rd party libraries
#   used in this example may not be suitable for your production use cases.
#   You should always independently verify the security and suitability of any
#   3rd party library used in your code.

# https://github.com/slazarov/python-signalr-client
from signalr_aio import Connection
from base64 import b64decode
from zlib import decompress, MAX_WBITS
import hashlib
import hmac
import json
import asyncio
import time
import uuid

URL = 'https://socket-v3.bittrex.com/signalr'
API_KEY = ''
API_SECRET = ''

HUB = None
LOCK = asyncio.Lock()
INVOCATION_EVENT = None
INVOCATION_RESPONSE = None


async def main():
    await connect()
    if(API_SECRET != ''):
        await authenticate()
    else:
        print('Authentication skipped because API key was not provided')
    await subscribe()
    forever = asyncio.Event()
    await forever.wait()


async def connect():
    global HUB
    connection = Connection(URL)
    HUB = connection.register_hub('c3')
    connection.received += on_message
    connection.error += on_error
    connection.start()
    print('Connected')


async def authenticate():
    timestamp = str(int(time.time()) * 1000)
    random_content = str(uuid.uuid4())
    content = timestamp + random_content
    signed_content = hmac.new(
        API_SECRET.encode(), content.encode(), hashlib.sha512).hexdigest()

    response = await invoke('Authenticate',
                            API_KEY,
                            timestamp,
                            random_content,
                            signed_content)

    if response['Success']:
        print('Authenticated')
        HUB.client.on('authenticationExpiring', on_auth_expiring)
    else:
        print('Authentication failed: ' + response['ErrorCode'])


async def subscribe():
    HUB.client.on('heartbeat', on_heartbeat)
#   HUB.client.on('trade', on_trade)
    HUB.client.on('candle', on_candle)
    channels = [
        'heartbeat',
        # 'trade_BTC-USD',
        'candle_BTC-USD_MINUTE_1'
    ]

    response = await invoke('Subscribe', channels)
    for i in range(len(channels)):
        if response[i]['Success']:
            print('Subscription to "' + channels[i] + '" successful')
        else:
            print('Subscription to "' +
                  channels[i] + '" failed: ' + response[i]['ErrorCode'])


async def invoke(method, *args):
    async with LOCK:
        global INVOCATION_EVENT
        INVOCATION_EVENT = asyncio.Event()
        HUB.server.invoke(method, *args)
        await INVOCATION_EVENT.wait()
        return INVOCATION_RESPONSE


async def on_message(**msg):
    global INVOCATION_RESPONSE
    if 'R' in msg:
        INVOCATION_RESPONSE = msg['R']
        INVOCATION_EVENT.set()


async def on_error(msg):
    print(msg)


async def on_heartbeat(msg):
    print('\u2661')


async def on_auth_expiring(msg):
    print('Authentication expiring...')
    asyncio.create_task(authenticate())


async def on_trade(msg):
    await print_message('Trade', msg)


async def on_candle(msg):
    await print_message('Candle', msg)


async def print_message(title, msg):
    decoded_msg = await process_message(msg[0])
    print(title + ': ' + json.dumps(decoded_msg, indent=2))


async def process_message(message):
    try:
        decompressed_msg = decompress(
            b64decode(message, validate=True), -MAX_WBITS)
    except SyntaxError:
        decompressed_msg = decompress(b64decode(message, validate=True))
    return json.loads(decompressed_msg.decode())

if __name__ == "__main__":
    asyncio.run(main())
