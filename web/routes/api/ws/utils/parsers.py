from common.helpers.datetimehelpers import seconds

def parse_ohlcv(data: dict, mls: bool=True) -> dict:
    '''
    JSON-serializes a single OHLCV datum

    The `time` attribute of the datum must represent a millisecond
        It can be of type `str` or `int`

    :params:
        `data`: dict - a single OHLCV datum
        `mls`: bool - whether return timestamp in milliseconds
            or seconds
    '''
    
    try:
        return {
            'time': int(data['time']) \
                if mls else seconds(int(data['time'])),
            'open': float(data['open']),
            'high': float(data['high']),
            'low': float(data['low']),
            'close': float(data['close']),
            'volume': float(data['volume'])
        }
    except Exception as exc:
        raise exc
