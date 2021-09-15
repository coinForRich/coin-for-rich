import pytest
import datetime
import time
import redis
from common.config.constants import (
    DEFAULT_DATETIME_STR_QUERY,
    REDIS_HOST, REDIS_USER, REDIS_PASSWORD
)
from common.helpers.datetimehelpers import *


# Fixtures
@pytest.fixture
def some_date():
    return datetime.datetime(2021, 1, 1, 0, 0, 0)

@pytest.fixture
def some_second():
    return 1609459200

@pytest.fixture
def some_millisecond():
    return 1609459200000

@pytest.fixture
def some_str():
    return "2021-01-01T00:00:00"

@pytest.fixture
def some_Redis():
    return redis.Redis(
        host=REDIS_HOST,
        username=REDIS_USER,
        password=REDIS_PASSWORD,
        decode_responses=True)

# Tests
@pytest.mark.beforepop
def test_milliseconds():
    assert milliseconds(10.0) == 10000

@pytest.mark.beforepop
def test_seconds():
    assert seconds(10000.0) == 10

@pytest.mark.beforepop
def test_microseconds_to_seconds():
    assert microseconds_to_seconds(1000000.0) == 1

@pytest.mark.beforepop
def test_datetime_to_seconds(some_date, some_second):
    assert datetime_to_seconds(some_date) == float(some_second)

@pytest.mark.beforepop
def test_datetime_to_milliseconds(some_date, some_millisecond):
    assert datetime_to_milliseconds(some_date) == float(some_millisecond)

@pytest.mark.beforepop
def test_milliseconds_to_datetime(some_millisecond, some_date):
    assert milliseconds_to_datetime(some_millisecond) == some_date

@pytest.mark.beforepop
def test_str_to_datetime(some_str, some_date):
    assert str_to_datetime(
        some_str, DEFAULT_DATETIME_STR_QUERY) == some_date

@pytest.mark.beforepop
def test_datetime_to_str(some_date, some_str):
    assert datetime_to_str(
        some_date, DEFAULT_DATETIME_STR_QUERY) == some_str

@pytest.mark.beforepop
def test_milliseconds_to_str(some_millisecond, some_str):
    assert milliseconds_to_str(
        some_millisecond, DEFAULT_DATETIME_STR_QUERY) == some_str

@pytest.mark.beforepop
def test_str_to_milliseconds(some_str, some_millisecond):
    assert str_to_milliseconds(
        some_str, DEFAULT_DATETIME_STR_QUERY) == some_millisecond

@pytest.mark.beforepop
def test_str_to_seconds(some_str, some_second):
    assert str_to_seconds(
        some_str, DEFAULT_DATETIME_STR_QUERY) == some_second

@pytest.mark.beforepop
def test_list_days_fromto():
    test_days = [
        datetime.datetime(2021, 1, 1),
        datetime.datetime(2021, 1, 2),
        datetime.datetime(2021, 1, 3),
        datetime.datetime(2021, 1, 4),
        datetime.datetime(2021, 1, 5),
    ]
    
    for i, d in enumerate(list_days_fromto(
        datetime.datetime(2021, 1, 1),
        datetime.datetime(2021, 1, 5)
    )):
        assert d == test_days[i]

@pytest.mark.beforepop
def test_redis_time(some_Redis):
    '''
    See if `redis_time` is correct within 1% variance
    '''
    
    start = redis_time(some_Redis)
    time.sleep(0.25)
    end = redis_time(some_Redis)
    assert (end - start >= 0.2525 or end - start >= 0.2475)
