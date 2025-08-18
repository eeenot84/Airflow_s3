"""
Конфигурация для ETL pipeline
Демонстрирует использование pydantic для валидации конфигурации
"""

from pydantic import BaseSettings, Field, validator
from typing import Optional, Dict, Any
import os


class DatabaseConfig(BaseSettings):
    """Конфигурация базы данных"""
    host: str = Field(default="localhost", env="DB_HOST")
    port: int = Field(default=5432, env="DB_PORT")
    database: str = Field(default="etl_demo", env="DB_NAME")
    username: str = Field(default="postgres", env="DB_USER")
    password: str = Field(default="password", env="DB_PASSWORD")
    
    @validator('port')
    def validate_port(cls, v):
        if not 1 <= v <= 65535:
            raise ValueError('Port must be between 1 and 65535')
        return v
    
    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"


class ETLConfig(BaseSettings):
    """Основная конфигурация ETL"""
    # Источники данных
    data_sources: Dict[str, str] = {
        "users": "https://jsonplaceholder.typicode.com/users",
        "posts": "https://jsonplaceholder.typicode.com/posts",
        "comments": "https://jsonplaceholder.typicode.com/comments"
    }
    
    # Настройки обработки
    batch_size: int = Field(default=1000, ge=1, le=10000)
    max_concurrent_requests: int = Field(default=10, ge=1, le=50)
    retry_attempts: int = Field(default=3, ge=1, le=10)
    
    # Пути для данных
    raw_data_path: str = Field(default="./data/raw")
    processed_data_path: str = Field(default="./data/processed")
    logs_path: str = Field(default="./logs")
    
    # База данных
    database: DatabaseConfig = DatabaseConfig()
    
    class Config:
        env_file = ".env"
        case_sensitive = False


# Глобальная конфигурация
config = ETLConfig()