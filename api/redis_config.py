import redis

# REDIS_URL = "redis://cache:6379/0"

REDIS_URL_KUBERNETES = "redis://redis-service:6379/0"

# redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)

redis_client = redis.Redis.from_url(REDIS_URL_KUBERNETES, decode_responses=True)
