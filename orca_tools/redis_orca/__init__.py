import redis


def get_connection(*, should_decode: bool = True) -> redis.Redis:
    return redis.Redis(
        host="redis",
        port=6379,
        db=0,
        decode_responses=should_decode,
    )
