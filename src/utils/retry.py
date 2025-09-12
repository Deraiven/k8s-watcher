"""
Retry utilities
"""
import asyncio
from functools import wraps
from typing import Any, Callable, TypeVar, Union

from .logger import setup_logger

logger = setup_logger(__name__)

T = TypeVar('T')


def async_retry(
    max_tries: int = 3,
    backoff_factor: float = 2.0,
    exceptions: tuple = (Exception,),
    exclude_exceptions: tuple = ()
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator for retrying async functions
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exception = None
            for attempt in range(max_tries):
                try:
                    return await func(*args, **kwargs)
                except exclude_exceptions as e:
                    # Don't retry for excluded exceptions
                    logger.debug(f"{func.__name__} raised excluded exception: {e}")
                    raise
                except exceptions as e:
                    last_exception = e
                    if attempt < max_tries - 1:
                        wait_time = backoff_factor ** attempt
                        logger.warning(
                            f"{func.__name__} failed (attempt {attempt + 1}/{max_tries}): {e}. "
                            f"Retrying in {wait_time}s..."
                        )
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error(f"{func.__name__} failed after {max_tries} attempts: {e}")
            
            raise last_exception
        
        return wrapper
    return decorator