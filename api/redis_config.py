import redis

REDIS_URL = "redis://cache:6379/0"

redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)