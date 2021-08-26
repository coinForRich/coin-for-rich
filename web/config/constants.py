PSQL_BATCHSIZE = 10000
OHLCV_INTERVALS = [
    '1m', '5m', '15m', '30m', '1h', '3h', '6h', '12h', '1D', '7D', '14D', '1M'
]
WS_SEND_EVENT_TYPES = ["subscribe", "unsubscribe"]
WS_SEND_REDIS_KEY = "ws_send_{exchange}{delimiter}{base_id}{delimiter}{quote_id}"
