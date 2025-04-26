from pyrate_limiter import RedisBucket, Rate, Duration, Limiter, BucketFullException
import os
# Using synchronous redis
from redis import ConnectionPool
from redis import Redis
from nexushub.utils import Log
import asyncio
logger = Log.get_logger()

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
REDIS_DB = os.getenv("REDIS_DB")



rates = [Rate(2400, Duration.MINUTE)]

pool = ConnectionPool.from_url(f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}")
redis_db = Redis(connection_pool=pool)
bucket_key = "binance"
bucket = RedisBucket.init(rates, redis_db, bucket_key)

limiter = Limiter(bucket, max_delay=100000)


async def request_handler(request):
    limiter.try_acquire(name='limiter')
    logger.info(f"request {request} success")


logger.info("start")
async def main():
    tasks = []
    for request in range(4800):
        tasks.append(request_handler(request))
    await asyncio.gather(*tasks)



if __name__ == "__main__":
    asyncio.run(main())
