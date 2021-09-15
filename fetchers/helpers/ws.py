# Helpers for WS fetchers

from fetchers.config.constants import WS_SUB_REDIS_KEY, WS_SERVE_REDIS_KEY


def make_sub_val(t, o, h, l, c, v, d) -> str:
    '''
    Serializes OHLCV into sub value

    For use of the WS updater
    
    :params:
        `t`: timestamp
        `d`: delimiter
    '''

    return f'{t}{d}{o}{d}{h}{d}{l}{d}{c}{d}{v}'

def make_sub_redis_key(exch: str, base: str, quote: str, delimiter: str) -> str:
    '''
    Makes sub Redis key for the e-b-q combination

    Can be of use when a query is submitted to view chart
        of a symbol (i.e., e-b-q combination);
        We can then indicate if the symbol is actively traded
    
    :params:
        `exch`: exchange name
        `base`: base id
        `quote`: quote id
    '''

    return WS_SUB_REDIS_KEY.format(
        exchange = exch,
        base_id = base,
        quote_id = quote,
        delimiter = delimiter
    )

def make_send_redis_key(exch: str, base: str, quote: str, delimiter: str) -> str:
    '''
    Makes send/serve Redis key for the e-b-q combination
    
    :params:
        `exch`: exchange name
        `base`: base id
        `quote`: quote id
    '''

    return WS_SERVE_REDIS_KEY.format(
        exchange = exch,
        base_id = base,
        quote_id = quote,
        delimiter = delimiter
    )
