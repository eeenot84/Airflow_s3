"""
Базовые классы для экстракторов данных
Демонстрирует использование ABC, контекстных менеджеров и async/await
"""

from abc import ABC, abstractmethod
from typing import AsyncGenerator, Dict, Any, Optional, List
import aiohttp
import asyncio
from loguru import logger
from ..utils.decorators import timing_decorator, retry_decorator, error_handler
from ..config import config


class BaseExtractor(ABC):
    """Базовый класс для всех экстракторов данных"""
    
    def __init__(self, source_name: str):
        self.source_name = source_name
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        """Асинхронный контекстный менеджер - вход"""
        connector = aiohttp.TCPConnector(limit=config.max_concurrent_requests)
        timeout = aiohttp.ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        logger.info(f"Initialized {self.__class__.__name__} for {self.source_name}")
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Асинхронный контекстный менеджер - выход"""
        if self.session:
            await self.session.close()
            logger.info(f"Closed session for {self.source_name}")
    
    @abstractmethod
    async def extract(self) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Абстрактный метод для извлечения данных
        Должен возвращать асинхронный генератор
        """
        pass
    
    @abstractmethod
    async def validate_data(self, data: Dict[str, Any]) -> bool:
        """
        Абстрактный метод для валидации данных
        """
        pass
    
    @retry_decorator(max_attempts=3, delay=1.0, backoff=2.0)
    @timing_decorator
    async def _fetch_data(self, url: str, **kwargs) -> Dict[str, Any]:
        """
        Базовый метод для получения данных по HTTP
        Использует декораторы для ретраев и измерения времени
        """
        if not self.session:
            raise RuntimeError("Session not initialized. Use async context manager.")
        
        logger.debug(f"Fetching data from {url}")
        
        async with self.session.get(url, **kwargs) as response:
            response.raise_for_status()
            data = await response.json()
            logger.debug(f"Successfully fetched {len(data) if isinstance(data, list) else 1} records from {url}")
            return data
    
    async def _fetch_paginated_data(self, base_url: str, page_size: int = 100) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Метод для получения пагинированных данных
        Демонстрирует работу с асинхронными генераторами
        """
        page = 1
        while True:
            try:
                url = f"{base_url}?_page={page}&_limit={page_size}"
                data = await self._fetch_data(url)
                
                if not data:
                    logger.info(f"No more data on page {page}, stopping pagination")
                    break
                
                if isinstance(data, list):
                    for item in data:
                        if await self.validate_data(item):
                            yield item
                else:
                    if await self.validate_data(data):
                        yield data
                
                page += 1
                
                # Небольшая задержка между запросами
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Error fetching page {page}: {e}")
                break


class HTTPExtractor(BaseExtractor):
    """
    Конкретная реализация экстрактора для HTTP API
    """
    
    def __init__(self, source_name: str, url: str, headers: Optional[Dict[str, str]] = None):
        super().__init__(source_name)
        self.url = url
        self.headers = headers or {}
    
    async def extract(self) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Извлекает данные из HTTP API
        """
        try:
            data = await self._fetch_data(self.url, headers=self.headers)
            
            # Если данные - это список, возвращаем каждый элемент отдельно
            if isinstance(data, list):
                for item in data:
                    if await self.validate_data(item):
                        yield item
            else:
                # Если данные - это один объект
                if await self.validate_data(data):
                    yield data
                    
        except Exception as e:
            logger.error(f"Failed to extract data from {self.url}: {e}")
            raise
    
    @error_handler(default_return_value=False, log_traceback=False)
    async def validate_data(self, data: Dict[str, Any]) -> bool:
        """
        Базовая валидация данных
        Проверяет, что данные не пустые и содержат необходимые поля
        """
        if not data:
            return False
        
        # Проверяем, что данные содержат хотя бы одно поле
        if not isinstance(data, dict) or len(data) == 0:
            return False
        
        logger.debug(f"Data validation passed for record with keys: {list(data.keys())}")
        return True


class BatchExtractor(BaseExtractor):
    """
    Экстрактор для пакетной обработки данных
    Демонстрирует работу с семафорами для ограничения конкурентности
    """
    
    def __init__(self, source_name: str, urls: List[str], batch_size: int = 10):
        super().__init__(source_name)
        self.urls = urls
        self.batch_size = batch_size
        self.semaphore = asyncio.Semaphore(config.max_concurrent_requests)
    
    async def extract(self) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Извлекает данные из нескольких источников параллельно
        """
        # Разбиваем URLs на батчи
        for i in range(0, len(self.urls), self.batch_size):
            batch_urls = self.urls[i:i + self.batch_size]
            
            # Создаем задачи для параллельного выполнения
            tasks = [self._fetch_with_semaphore(url) for url in batch_urls]
            
            # Выполняем батч параллельно
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Обрабатываем результаты
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Error in batch processing: {result}")
                    continue
                
                if isinstance(result, list):
                    for item in result:
                        if await self.validate_data(item):
                            yield item
                elif await self.validate_data(result):
                    yield result
    
    async def _fetch_with_semaphore(self, url: str) -> Dict[str, Any]:
        """
        Получает данные с использованием семафора для ограничения конкурентности
        """
        async with self.semaphore:
            return await self._fetch_data(url)
    
    async def validate_data(self, data: Dict[str, Any]) -> bool:
        """Базовая валидация для батчевого экстрактора"""
        return isinstance(data, dict) and len(data) > 0