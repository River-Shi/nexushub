import asyncio
import time
from collections import deque
from nexushub.utils import Log
import redis.asyncio as redis

class AsyncThrottler:
    """
    An asynchronous throttler based on cumulative weight over a fixed time period.

    Designed to mimic Binance's request weight limit (e.g., 2400 weight per minute).
    """
    def __init__(self, rate_limit: int, period: float = 60.0):
        """
        Initializes the throttler.

        Args:
            rate_limit: The maximum cumulative weight allowed within the period.
            period: The time period in seconds (default is 60 for per minute).
        """
        if rate_limit <= 0:
            raise ValueError("Rate limit must be positive")
        if period <= 0:
            raise ValueError("Period must be positive")

        self.rate_limit = rate_limit
        self.period = period

        self._current_weight = 0
        # Use monotonic clock for reliable interval measurement
        self._window_start_time = time.monotonic()
        self._lock = asyncio.Lock()
        self._waiters: deque[asyncio.Future] = deque() # Keep track of waiting tasks for fairness (optional but good)
        self._logger = Log.get_logger()

        self._logger.debug(f"Throttler initialized: Limit={rate_limit} weight / {period} seconds")

    def _check_and_reset_window(self) -> bool:
        """Internal method to check if the window needs resetting. Must be called under lock."""
        now = time.monotonic()
        elapsed = now - self._window_start_time # unit: seconds
        if elapsed >= self.period:
            lapsed_periods = int(elapsed // self.period)
            self._window_start_time += lapsed_periods * self.period
            old_weight = self._current_weight
            self._current_weight = 0
            self._logger.debug(f"Time window reset. Start: {self._window_start_time:.2f}, Old Weight: {old_weight}")
            return True
        return False

    def _notify_waiters(self):
        """Notify the first waiter, if any."""
        if self._waiters:
            waiter_future = self._waiters[0]
            if not waiter_future.done():
                 # Set result to trigger the waiter to re-check
                waiter_future.set_result(True)

    async def acquire(self, weight: int = 1) -> None:
        """
        Acquires permission to proceed, waiting if necessary.

        Args:
            weight: The weight cost of the operation to be performed.

        Raises:
            ValueError: If the requested weight exceeds the total rate limit.
        """
        if weight < 0:
            raise ValueError("Weight cannot be negative")
        if weight > self.rate_limit:
            raise ValueError(
                f"Requested weight {weight} exceeds the total rate limit {self.rate_limit} per period."
            )

        my_waiter_future = None

        while True:
            async with self._lock:
                # Check and potentially reset the time window
                was_reset = self._check_and_reset_window()

                # Check if the current request can be accommodated
                if self._current_weight + weight <= self.rate_limit:
                    # Yes, grant permission
                    self._current_weight += weight
                    self._logger.debug(f"Acquired {weight} weight. Current total: {self._current_weight}/{self.rate_limit}")

                    # If this task was waiting, remove its future from the queue
                    if my_waiter_future and my_waiter_future in self._waiters:
                        try:
                            self._waiters.remove(my_waiter_future)
                        except ValueError:
                             # Can happen if notified and removed concurrently, safe to ignore
                             pass

                    # Notify the *next* waiter if there's still capacity after this acquisition
                    if was_reset:
                        self._notify_waiters()
                    return # Exit the loop and method

                # No, need to wait
                else:
                    now = time.monotonic()
                    time_until_reset = (self._window_start_time + self.period) - now
                    self._logger.debug(f"Limit reached ({self._current_weight}/{self.rate_limit}). "
                                 f"Need {weight} weight. Waiting for window reset in {time_until_reset:.2f}s.")

                    # Create a future for waiting if not already done
                    if my_waiter_future is None:
                        my_waiter_future = asyncio.get_running_loop().create_future()
                        self._waiters.append(my_waiter_future)

            # === Wait outside the lock ===
            try:
                # Wait for the calculated reset time
                await asyncio.wait_for(my_waiter_future, timeout=max(0, time_until_reset) + 0.01) # Small buffer
                # If we get here, it means we were notified, but this should never happen
                # since the window can only reset after the full period
                continue
            except asyncio.TimeoutError:
                # Timeout occurred, means the window should have reset.
                # Loop will re-acquire lock and check state again.
                if my_waiter_future in self._waiters:
                    try:
                        self._waiters.remove(my_waiter_future)
                        my_waiter_future = None
                    except ValueError:
                        pass # Already removed
                continue # Go back to the start of the while loop to re-check
            

    async def __aenter__(self, weight: int = 1) -> None:
        """
        Asynchronous context manager entry point.
        
        Args:
            weight: The weight cost of the operation to be performed.
            
        Returns:
            None
        """
        await self.acquire(weight)
        return None

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Asynchronous context manager exit point.
        No cleanup needed as the weight is automatically reset after the period.
        """
        pass
    
    
class RedisThrottler:
    """
    A distributed throttler based on Redis that limits cumulative weight across multiple processes.
    
    Uses Redis to track and limit API requests across different processes sharing the same key.
    """
    def __init__(self, rate_limit: int, period: float = 60.0, bucket_key: str = "default_throttler",
                 redis_url: str = "redis://:Quant2025!@localhost:6379/0"):
        """
        Initializes the Redis-based throttler.

        Args:
            rate_limit: The maximum cumulative weight allowed within the period.
            period: The time period in seconds (default is 60 for per minute).
            bucket_key: The Redis key used to identify this specific throttler.
            redis_url: URL for connecting to Redis.
        """
        if rate_limit <= 0:
            raise ValueError("Rate limit must be positive")
        if period <= 0:
            raise ValueError("Period must be positive")
        
        self.rate_limit = rate_limit
        self.period = period
        self.bucket_key = bucket_key
        self.redis_url = redis_url
        
        self._redis_client = None
        self._logger = Log.get_logger()
        self._local_lock = asyncio.Lock()
        self._waiters: deque[asyncio.Future] = deque()
        
        self._logger.debug(f"RedisThrottler initialized: Limit={rate_limit} weight / {period} seconds using key '{bucket_key}'")
    
    async def _get_redis(self) -> redis.Redis:
        """Get or create the Redis client."""
        if self._redis_client is None:
            self._redis_client = await redis.from_url(self.redis_url)
        return self._redis_client
    
    async def _get_current_weight(self) -> int:
        """Get the current accumulated weight from Redis."""
        redis_client = await self._get_redis()
        value = await redis_client.get(self.bucket_key)
        if value is None:
            return 0
        return int(value)
    
    async def _check_and_reset_window(self) -> bool:
        """Check if the time window needs resetting and reset if needed."""
        redis_client = await self._get_redis()
        
        # Get the window start time
        window_key = f"{self.bucket_key}_window_start"
        window_start = await redis_client.get(window_key)
        
        now = time.time()
        
        # If window start doesn't exist, set it now
        if window_start is None:
            await redis_client.set(window_key, str(now), ex=int(self.period * 2))
            return False
        
        # Check if window needs resetting
        window_start = float(window_start)
        elapsed = now - window_start
        
        if elapsed >= self.period:
            # Update window start time
            lapsed_periods = int(elapsed // self.period)
            new_start = window_start + lapsed_periods * self.period
            
            # Use a transaction to reset the weight and update window
            tr = redis_client.pipeline()
            tr.set(self.bucket_key, "0")
            tr.set(window_key, str(new_start), ex=int(self.period * 2))
            await tr.execute()
            
            self._logger.debug(f"Time window reset. New start: {new_start:.2f}")
            return True
            
        return False
    
    def _notify_waiters(self):
        """Notify the first waiter, if any."""
        if self._waiters:
            waiter_future = self._waiters[0]
            if not waiter_future.done():
                waiter_future.set_result(True)
    
    async def acquire(self, weight: int = 1) -> None:
        """
        Acquires permission to proceed, waiting if necessary.

        Args:
            weight: The weight cost of the operation to be performed.

        Raises:
            ValueError: If the requested weight exceeds the total rate limit.
        """
        if weight < 0:
            raise ValueError("Weight cannot be negative")
        if weight > self.rate_limit:
            raise ValueError(
                f"Requested weight {weight} exceeds the total rate limit {self.rate_limit} per period."
            )
        
        my_waiter_future = None
        redis_client = await self._get_redis()
        
        while True:
            async with self._local_lock:
                # Check and potentially reset the time window
                was_reset = await self._check_and_reset_window()
                
                # Get current weight
                current_weight = await self._get_current_weight()
                
                # Check if the current request can be accommodated
                if current_weight + weight <= self.rate_limit:
                    # Use Lua script for atomic increment
                    # This ensures we don't exceed the limit due to race conditions
                    lua_script = """
                    local current = redis.call('get', KEYS[1])
                    if current == false then current = 0 else current = tonumber(current) end
                    local new_weight = current + tonumber(ARGV[1])
                    if new_weight <= tonumber(ARGV[2]) then
                        redis.call('set', KEYS[1], new_weight)
                        return 1
                    else
                        return 0
                    end
                    """
                    
                    result = await redis_client.eval(
                        lua_script,
                        1,  # number of keys
                        self.bucket_key,  # key
                        weight,  # weight to add (ARGV[1])
                        self.rate_limit  # max weight (ARGV[2])
                    )
                    
                    if result == 1:
                        # Successfully acquired weight
                        self._logger.debug(f"Acquired {weight} weight. New total: {current_weight + weight}/{self.rate_limit}")
                        
                        # Remove this task's waiter if it exists
                        if my_waiter_future and my_waiter_future in self._waiters:
                            try:
                                self._waiters.remove(my_waiter_future)
                            except ValueError:
                                pass
                                
                        # Notify next waiter if window was reset
                        if was_reset:
                            self._notify_waiters()
                            
                        return
                
                # Need to wait for capacity
                window_key = f"{self.bucket_key}_window_start"
                window_start = float(await redis_client.get(window_key) or time.time())
                now = time.time()
                time_until_reset = (window_start + self.period) - now
                
                self._logger.debug(f"Limit reached. Need {weight} weight. Waiting for window reset in {max(0, time_until_reset):.2f}s.")
                
                # Create a future for waiting if not already done
                if my_waiter_future is None:
                    my_waiter_future = asyncio.get_running_loop().create_future()
                    self._waiters.append(my_waiter_future)
            
            # Wait outside the lock
            try:
                await asyncio.wait_for(my_waiter_future, timeout=max(0, time_until_reset) + 0.01)
                continue  # We were notified, try again
            except asyncio.TimeoutError:
                # Timeout occurred, window should have reset
                if my_waiter_future in self._waiters:
                    try:
                        self._waiters.remove(my_waiter_future)
                        my_waiter_future = None
                    except ValueError:
                        pass
                continue  # Try again
    
    async def __aenter__(self) -> None:
        """Asynchronous context manager entry."""
        await self.acquire()
        return None
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Asynchronous context manager exit."""
        pass
    
    async def cleanup(self) -> None:
        """Close Redis connections and clean up resources."""
        if self._redis_client:
            await self._redis_client.aclose()
            self._redis_client = None

async def fetch_order_data(throttler: AsyncThrottler, order_id: int):
    api_weight = 10  # Example weight for /api/order
    print(f"Task {order_id}: Attempting to acquire {api_weight} weight for order data...")
    await throttler.acquire(weight=api_weight)
    print(f"Task {order_id}: Acquired weight. Fetching order data...")
    # Simulate API call duration
    await asyncio.sleep(0.1)
    print(f"Task {order_id}: Fetched order data.")
    return f"Order data {order_id}"

async def fetch_ticker_data(throttler: AsyncThrottler, symbol: str):
    api_weight = 5  # Example weight for /api/ticker
    print(f"Task {symbol}: Attempting to acquire {api_weight} weight for ticker data...")
    await throttler.acquire(weight=api_weight)
    print(f"Task {symbol}: Acquired weight. Fetching ticker data...")
    # Simulate API call duration
    await asyncio.sleep(0.05)
    print(f"Task {symbol}: Fetched ticker data.")
    return f"Ticker data {symbol}"

async def main():
    # Binance limit: 2400 weight per minute
    # For testing, let's use a smaller limit and shorter period
    # test_limit = 50
    # test_period = 5.0 # seconds
    # throttler = AsyncWeightThrottler(rate_limit=test_limit, period=test_period)

    # Use Binance actual limits
    binance_limit = 100
    binance_period = 10 # seconds
    throttler = RedisThrottler(
        rate_limit=binance_limit, 
        period=binance_period,
        bucket_key="binance_api"
    )


    tasks = []
    # Create a mix of tasks with different weights
    for i in range(20): # Simulate 5 order fetches
        tasks.append(fetch_order_data(throttler, i + 1))

    # for i in range(20): # Simulate 20 ticker fetches
    #     tasks.append(fetch_ticker_data(throttler, f"SYM{i+1}"))

    start_time = time.time()
    await asyncio.gather(*tasks)
    end_time = time.time()

    # print("\nResults:")
    # for result in results:
    #     print(result)

    print(f"\nTotal time taken: {end_time - start_time:.2f} seconds")
    # With the 2400/60s limit, the 5 heavy tasks alone exceed the limit.
    # The total time should be slightly over 60 seconds if the last heavy task
    # had to wait for the window reset.
    await throttler.cleanup()

if __name__ == "__main__":
    # To see debug logs, uncomment this line:
    Log.setup_logger(log_level="DEBUG")
    asyncio.run(main())
