import asyncio
import time
from collections import deque
from nexushub.utils import Log
from multiprocessing import shared_memory, Lock
import numpy as np
from typing import Deque
from contextlib import contextmanager
from filelock import FileLock 
import os

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
    
    


class SharedThrottler:
    """
    A cross-process rate limiter based on shared memory.
    Ensures multiple processes share the same limiter by using the shared memory name.
    Shared memory structure: [window_start_time, current_weight, ref_count]
    """
    def __init__(
        self,
        rate_limit: int,
        period: float = 60.0,
        name: str = "nexushub_throttler"
    ):
        if rate_limit <= 0:
            raise ValueError("Rate limit must be positive")
        if period <= 0:
            raise ValueError("Period must be positive")
        if name.startswith("/"):
            raise ValueError("Shared memory name cannot start with '/'")
        
        self.rate_limit = rate_limit
        self.period = period
        self.shm_name = name
        self._logger = Log.get_logger()
        
        
        lock_file = f"/tmp/{name}.lock"
        self._lock = FileLock(lock_file)
        
        # waiting queue (in-process)
        self._waiters: Deque[asyncio.Future] = deque()
        
        # initialize or connect to the shared memory
        try:
            # Try to connect to existing shared memory first
            self._shm = shared_memory.SharedMemory(name=name)
            self._data = np.ndarray((3,), dtype=np.float64, buffer=self._shm.buf)
            with self._acquire_lock():
                self._data[2] += 1  # Increment reference count
            self._logger.debug(f"Connected to existing shared memory '{name}', ref_count={self._data[2]}")
        except FileNotFoundError:
            # If not exists, create new shared memory
            self._shm = shared_memory.SharedMemory(
                name=name,
                create=True,
                size=3 * 8  # 3 float64
            )
            self._data = np.ndarray((3,), dtype=np.float64, buffer=self._shm.buf)
            self._data[0] = time.time()  # window_start_time
            self._data[1] = 0.0  # current_weight
            self._data[2] = 1.0  # reference count
            self._logger.debug(f"Created new shared memory '{name}', ref_count=1")
        
        
    def _notify_waiters(self):
        """notify the waiting tasks"""
        if self._waiters:
            waiter = self._waiters[0]
            if not waiter.done():
                waiter.set_result(True)
            
    @contextmanager
    def _acquire_lock(self, timeout: float = 1.0):
        """get the process lock context manager"""
        acquired = self._lock.acquire(timeout=timeout)
        if not acquired:
            raise TimeoutError("Failed to acquire lock")
        try:
            yield
        finally:
            self._lock.release()
            
    def _check_and_reset_window(self) -> bool:
        """check and reset the time window"""
        now = time.time()
        window_start = self._data[0]
        
        if now - window_start >= self.period:
            self._data[0] = now
            old_weight = self._data[1]
            self._data[1] = 0.0
            self._logger.debug(
                f"Time window reset in shared memory '{self.shm_name}'. "
                f"Old weight: {old_weight}"
            )
            return True
        return False
            
    async def acquire(self, weight: int = 1) -> None:
        """acquire the weight permission"""
        if weight < 0:
            raise ValueError("Weight cannot be negative")
        if weight > self.rate_limit:
            raise ValueError(f"Weight {weight} exceeds limit {self.rate_limit}")
            
        my_waiter = None
        
        while True:
            try:
                with self._acquire_lock():
                    was_reset = self._check_and_reset_window()
                    current_weight = self._data[1]
                    
                    if current_weight + weight <= self.rate_limit:
                        self._data[1] = current_weight + weight
                        self._logger.debug(
                            f"[{self.shm_name}] Acquired {weight} weight. "
                            f"Current total: {current_weight + weight}/{self.rate_limit}"
                        )
                        
                        if my_waiter and my_waiter in self._waiters:
                            try:
                                self._waiters.remove(my_waiter)
                            except ValueError:
                                pass
                            
                        if was_reset:
                            self._notify_waiters()
                            
                        return
                    else:
                        window_start = self._data[0]
                        wait_time = max(0, window_start + self.period - time.time())
                        
                        if my_waiter is None:
                            my_waiter = asyncio.get_running_loop().create_future()
                            self._waiters.append(my_waiter)
                            
                        self._logger.debug(
                            f"[{self.shm_name}] Limit reached ({current_weight}/{self.rate_limit}). "
                            f"Need {weight} weight. "
                            f"Waiting {wait_time:.2f}s for reset"
                        )
                    
            except TimeoutError:
                self._logger.debug(f"[{self.shm_name}] Timeout error, waiting for 0.1s")
                await asyncio.sleep(0.1)
                continue
                
            try:
                await asyncio.wait_for(
                    my_waiter,
                    timeout=max(0, wait_time) + 0.01
                )
            except asyncio.TimeoutError:
                continue
                
    async def __aenter__(self):
        await self.acquire()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
    
    def cleanup(self):
        """Close the shared memory connection and update reference count."""
        if hasattr(self, '_shm'):
            try:
                with self._acquire_lock():
                    self._data[2] -= 1  # Decrement reference count
                    ref_count = self._data[2]
                    self._logger.debug(f"Decremented ref_count to {ref_count} for '{self.shm_name}'")
                    
                    if ref_count <= 0:
                        self._shm.unlink()
                        self._logger.debug(f"Unlinked shared memory '{self.shm_name}' as last user")
                    
                self._shm.close()
                self._logger.debug(f"Closed shared memory connection '{self.shm_name}'")
            except Exception as e:
                self._logger.error(f"Error in close: {e}")
    
    
    

# --- Example Usage ---

# Simulate different API calls with different weights
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
    throttler = SharedThrottler(rate_limit=binance_limit, period=binance_period)


    tasks = []
    # Create a mix of tasks with different weights
    for i in range(11): # Simulate 5 order fetches
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
    throttler.cleanup()

if __name__ == "__main__":
    # To see debug logs, uncomment this line:
    Log.setup_logger(log_level="DEBUG")
    asyncio.run(main())
