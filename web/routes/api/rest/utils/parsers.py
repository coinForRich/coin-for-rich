import logging
from common.helpers.numbers import round_decimal
from common.helpers.datetimehelpers import (
    datetime_to_seconds,
    datetime_to_milliseconds
)


def parse_ohlcv(ohlcvs: list, mls: bool) -> list:
    '''
    Parses OHLCV received from API endpoint
        for web chart view by converting timestamp
        into seconds
    
    :params:
        `ohlcvs`: list - OHLCVs received
        `mls`: bool - whether to convert timestamps to milliseconds
            if true: convert to milliseconds
            if false: convert to seconds
    '''

    ret = []
    if ohlcvs:
        try:
            # ohlcv.sort(key = lambda x: x.time)
            ret = [
                {
                    'time': int(datetime_to_milliseconds(o.time)) if mls \
                        else int(datetime_to_seconds(o.time)),
                    'open': round_decimal(o.open),
                    'high': round_decimal(o.high),
                    'low': round_decimal(o.low),
                    'close': round_decimal(o.close),
                    'volume': round_decimal(o.volume)
                }
                for o in ohlcvs
            ]
        except TypeError as exc:
            logging.warning(f"parse_ohlcv: EXCEPTION: {exc}")
    return ret
