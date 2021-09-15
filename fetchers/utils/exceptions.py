from websockets.exceptions import (
    ConnectionClosed,
    ConnectionClosedOK,
    InvalidStatusCode
)


class UnsuccessfulConnection(Exception):
    '''
    Unsuccessful connection to an endpoint (e.g., ws)
    '''
    pass

class MaximumRetriesReached(Exception):
    '''
    Maximum retries reached while fetching an endpoint
    '''
    pass

class UnsuccessfulDatabaseInsert(Exception):
    '''
    Unsuccessful insert to db
    '''
    pass
