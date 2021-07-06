import json


def redis_pipe_rpush(redis_client, key, vals, serialize=False):
    '''
    Redis serialize (optional) and rpush using pipeline
    params:
        `redis_client`: Redis client obj
        `key`: string - Redis key
        `vals`: iterable of serialized values
    '''

    pipe = redis_client.pipeline()
    for val in vals:
        if serialize:
            pipe.rpush(key, json.dumps(val))
        else:
            pipe.rpush(key, val)
    pipe.execute()
