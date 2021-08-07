from websockets.exceptions import ConnectionClosed, ConnectionClosedOK


class UnsuccessfulConnection(Exception):
    pass

class MaximumRetriesReached(Exception):
    pass