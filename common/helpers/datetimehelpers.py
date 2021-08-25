# This module contains common datetime helpers

import datetime
from redis import Redis


def milliseconds(seconds):
    '''
    returns milliseconds
    :params:
        `seconds`: float or int
    '''
    return int(seconds * 1000)

def seconds(mls):
    '''
    returns seconds
    :params:
        `mls`: float or int of milliseconds
    '''
    return int(mls / 1000)

def microseconds_to_seconds(mic: float):
    '''
    returns seconds from `mic` microseconds
    
    :params:
        `mic`: float of microseconds
    '''
    return mic / 1000000

def datetime_to_seconds(dt):
    '''
    converts a datetime.datetime object to seconds, represented in float
    :params:
        `dt`: datetime object
    '''
    return dt.timestamp()

def datetime_to_milliseconds(dt: datetime.datetime) -> int:
    '''
    converts a datetime.datetime object to milliseconds, represented in int
    :params:
        `dt`: datetime object
    '''
    return milliseconds(datetime_to_seconds(dt))

def milliseconds_to_datetime(mls: int) -> datetime.datetime:
    '''
    converts a millisecond timestamp into datetime object
    :params:
        `mls`: int (milliseconds)
    '''
    return datetime.datetime.fromtimestamp(mls/1000)

def str_to_datetime(s, f):
    '''
    converts a string of format `f` to datetime obj
    :params:
        `s`: string - representing datetime
        `f`: string - time format
    '''
    return datetime.datetime.strptime(s, f)

def datetime_to_str(dt, f):
    '''
    converts a datetime object into a string of format `f`
    :params:
        `dt`: datetime obj
        `f`: string - time format
    '''
    return dt.strftime(f)

def milliseconds_to_str(mls, f):
    '''
    converts a millisecond timestamp into string of format `f`
    :params:
        `mls`: int (milliseconds)
        `f`: string - time format
    '''

    return datetime_to_str(milliseconds_to_datetime(mls), f)

def str_to_milliseconds(s, f):
    '''
    converts a string of format `f` to milliseconds
    :params:
        `s`: datetime string
        `f`: string - time format
    '''

    return datetime_to_milliseconds(str_to_datetime(s, f))

def str_to_seconds(s, f):
    '''
    converts a string of format `f` to seconds
    :params:
        `s`: datetime string
        `f`: string - time format
    '''

    return seconds(str_to_milliseconds(s, f))

def list_days_fromto(start_date, end_date):
    '''
    generates the days between two days (inclusive)
    :params:
        `start_date`: datetime obj
        `end_date`: datetime obj
    '''

    for n in range((end_date - start_date).days+1):
        yield start_date + datetime.timedelta(days=n)

def redis_time(r: Redis) -> float:
    '''
    generates the time in the Redis server - in seconds,
        including fractions of a second
    
    :params:
        `r`: Redis client object
    '''
    
    secs, mics = r.time()
    return float(secs) + microseconds_to_seconds(float(mics))
