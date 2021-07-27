from websockets.exceptions import ConnectionClosedOK


class UnsuccessfulConnection(Exception):
    pass

class MaximumRetriesReached(Exception):
    pass