"""
Продвинутые декораторы для ETL pipeline
Демонстрирует создание собственных декораторов для логирования, ретраев и метрик
"""

import time
import asyncio
from functools import wraps
from typing import Callable, Any, Optional, Union
from loguru import logger
import traceback


def timing_decorator(func: Callable) -> Callable:
    """
    Декоратор для измерения времени выполнения функции
    Работает как с синхронными, так и с асинхронными функциями
    """
    @wraps(func)
    async def async_wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = await func(*args, **kwargs)
            execution_time = time.time() - start_time
            logger.info(f"Async function {func.__name__} executed in {execution_time:.2f} seconds")
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Async function {func.__name__} failed after {execution_time:.2f} seconds: {e}")
            raise

    @wraps(func)
    def sync_wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            logger.info(f"Sync function {func.__name__} executed in {execution_time:.2f} seconds")
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Sync function {func.__name__} failed after {execution_time:.2f} seconds: {e}")
            raise

    # Определяем, асинхронная ли функция
    if asyncio.iscoroutinefunction(func):
        return async_wrapper
    else:
        return sync_wrapper


def retry_decorator(max_attempts: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """
    Декоратор для повторных попыток выполнения функции
    
    Args:
        max_attempts: Максимальное количество попыток
        delay: Начальная задержка между попытками (секунды)
        backoff: Множитель для увеличения задержки
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            current_delay = delay
            last_exception = None
            
            for attempt in range(1, max_attempts + 1):
                try:
                    logger.info(f"Attempt {attempt}/{max_attempts} for {func.__name__}")
                    result = await func(*args, **kwargs)
                    if attempt > 1:
                        logger.info(f"Success on attempt {attempt} for {func.__name__}")
                    return result
                except Exception as e:
                    last_exception = e
                    logger.warning(f"Attempt {attempt}/{max_attempts} failed for {func.__name__}: {e}")
                    
                    if attempt < max_attempts:
                        logger.info(f"Retrying in {current_delay} seconds...")
                        await asyncio.sleep(current_delay)
                        current_delay *= backoff
            
            logger.error(f"All {max_attempts} attempts failed for {func.__name__}")
            raise last_exception

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            current_delay = delay
            last_exception = None
            
            for attempt in range(1, max_attempts + 1):
                try:
                    logger.info(f"Attempt {attempt}/{max_attempts} for {func.__name__}")
                    result = func(*args, **kwargs)
                    if attempt > 1:
                        logger.info(f"Success on attempt {attempt} for {func.__name__}")
                    return result
                except Exception as e:
                    last_exception = e
                    logger.warning(f"Attempt {attempt}/{max_attempts} failed for {func.__name__}: {e}")
                    
                    if attempt < max_attempts:
                        logger.info(f"Retrying in {current_delay} seconds...")
                        time.sleep(current_delay)
                        current_delay *= backoff
            
            logger.error(f"All {max_attempts} attempts failed for {func.__name__}")
            raise last_exception

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


def error_handler(default_return_value: Any = None, log_traceback: bool = True):
    """
    Декоратор для обработки ошибок с возможностью возврата значения по умолчанию
    
    Args:
        default_return_value: Значение, возвращаемое в случае ошибки
        log_traceback: Логировать ли полный traceback
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                if log_traceback:
                    logger.error(f"Error in {func.__name__}: {e}\n{traceback.format_exc()}")
                else:
                    logger.error(f"Error in {func.__name__}: {e}")
                return default_return_value

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if log_traceback:
                    logger.error(f"Error in {func.__name__}: {e}\n{traceback.format_exc()}")
                else:
                    logger.error(f"Error in {func.__name__}: {e}")
                return default_return_value

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


def validate_input(**validators):
    """
    Декоратор для валидации входных параметров
    
    Example:
        @validate_input(user_id=lambda x: isinstance(x, int) and x > 0)
        def get_user(user_id):
            pass
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Получаем имена параметров функции
            import inspect
            sig = inspect.signature(func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()
            
            # Валидируем параметры
            for param_name, validator_func in validators.items():
                if param_name in bound_args.arguments:
                    value = bound_args.arguments[param_name]
                    if not validator_func(value):
                        raise ValueError(f"Validation failed for parameter '{param_name}' with value '{value}'")
            
            return func(*args, **kwargs)
        
        return wrapper
    
    return decorator