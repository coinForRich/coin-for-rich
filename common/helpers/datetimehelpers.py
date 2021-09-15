# This module contains common datetime helpers

import datetime
from typing import Generator, Union, Any
from redis import Redis


def milliseconds(seconds: Union[float, int]) -> int:
    '''
    returns milliseconds from seconds

    :params:
        `seconds`: float or int of seconds
    '''
    return int(seconds * 1000)

def seconds(mls: Union[float, int]) -> int:
    '''
    returns seconds from milliseconds

    :params:
        `mls`: float or int of milliseconds
    '''
    return int(mls / 1000)

def microseconds_to_seconds(mic: Union[float, int]) -> float:
    '''
    returns seconds from `mic` microseconds
    
    :params:
        `mic`: float or int of microseconds
    '''
    return mic / 1000000

def datetime_to_seconds(dt: datetime.datetime) -> float:
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

def milliseconds_to_datetime(mls: Union[float, int]) -> datetime.datetime:
    '''
    converts a millisecond timestamp into datetime object
    
    :params:
        `mls`: float or int (milliseconds)
    '''
    return datetime.datetime.fromtimestamp(mls/1000)

def str_to_datetime(s: str, f: str) -> datetime.datetime:
    '''
    converts a string of format `f` to datetime obj
    
    :params:
        `s`: string - representing datetime
        `f`: string - time format
    '''
    return datetime.datetime.strptime(s, f)

def datetime_to_str(dt: datetime.datetime, f: str) -> str:
    '''
    converts a datetime object into a string of format `f`
    
    :params:
        `dt`: datetime obj
        `f`: string - time format
    '''
    return dt.strftime(f)

def milliseconds_to_str(mls: int, f: str) -> str:
    '''
    converts a millisecond timestamp into string of format `f`
    
    :params:
        `mls`: int (milliseconds)
        `f`: string - time format
    '''

    return datetime_to_str(milliseconds_to_datetime(mls), f)

def str_to_milliseconds(s: str, f: str) -> int:
    '''
    converts a string of format `f` to milliseconds
    
    :params:
        `s`: datetime string
        `f`: string - time format
    '''

    return datetime_to_milliseconds(str_to_datetime(s, f))

def str_to_seconds(s: str, f: str) -> int:
    '''
    converts a string of format `f` to seconds
    
    :params:
        `s`: datetime string
        `f`: string - time format
    '''

    return seconds(str_to_milliseconds(s, f))

def list_days_fromto(
        start_date: datetime.datetime,
        end_date: datetime.datetime
    ) -> Generator[datetime.datetime, Any, Any]:
    '''
    generates the days between two days (inclusive)
    
    :params:
        `start_date`: datetime obj
        `end_date`: datetime obj
    '''

    for n in range((end_date - start_date).days + 1):
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
