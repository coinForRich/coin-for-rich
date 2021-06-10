# This module contains common db helpers

import json


def redis_pipe_rpush(redis_client, key, vals, serialize=False):
    '''
    Redis serialize (optional) and rpush using pipeline
    params:
        `redis_client`: Redis client obj
        `key`: string - Redis key
        `vals`: iterable of serialized values
    '''

    with redis_client.pipeline() as pipe:
        for val in vals:
            if serialize:
                pipe.rpush(key, json.dumps(val))
            else:
                pipe.rpush(key, val)
        pipe.execute()

def redis_pipe_sadd(redis_client, key, vals, serialize=False):
    '''
    Redis serialize (optional) and rpush using pipeline
    params:
        `redis_client`: Redis client obj
        `key`: string - Redis key
        `vals`: iterable of serialized values
    '''

    with redis_client.pipeline() as pipe:
        for val in vals:
            if serialize:
                pipe.sadd(key, json.dumps(val))
            else:
                pipe.sadd(key, val)
        pipe.execute()
