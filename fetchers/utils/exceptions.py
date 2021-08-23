from websockets.exceptions import (
    ConnectionClosed,
    ConnectionClosedOK,
    InvalidStatusCode
)


class UnsuccessfulConnection(Exception):
    pass

class MaximumRetriesReached(Exception):
    pass