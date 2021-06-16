import datetime


def milliseconds(seconds):
    '''
    returns milliseconds
    params:
        `seconds`: float or int
    '''
    return int(seconds) * 1000

def datetime_to_seconds(dt):
    '''
    converts a datetime.datetime object to seconds, represented in float
    params:
        `dt`: datetime object
    '''
    return dt.timestamp()

def datetime_to_milliseconds(dt):
    '''
    converts a datetime.datetime object to milliseconds, represented in int
    params:
        `dt`: datetime object
    '''
    return milliseconds(datetime_to_seconds(dt))

def milliseconds_to_datetime(mls):
    '''
    converts a millisecond timestamp into datetime object
    params:
        `mls`: int (milliseconds)
    '''
    return datetime.datetime.fromtimestamp(mls/1000)

def str_to_datetime(s, f):
    '''
    converts a string of format `f` to datetime obj
    params:
        `s`: string - representing datetime
        `f`: string - time format
    '''
    return datetime.datetime.strptime(s, f)

def datetime_to_str(dt, f):
    '''
    converts a datetime object into a string of format `f`
    params:
        `dt`: datetime obj
        `f`: string - time format
    '''
    return dt.strftime(f)