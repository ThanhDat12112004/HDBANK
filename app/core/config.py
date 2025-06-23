from pydantic_settings import BaseSettings
import os
from dotenv import load_dotenv
import logging.config

load_dotenv()

# Logging configuration
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "default",
            "level": "INFO",
        },
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "default",
            "filename": "app.log",
            "maxBytes": 1024 * 1024,  # 1MB
            "backupCount": 5,
            "level": "INFO",
        },
    },
    "loggers": {
        "": {  # Root logger
            "handlers": ["console", "file"],
            "level": "INFO",
            "encoding": "utf-8", 
        },
    },
}

class Settings(BaseSettings):
    API_V1_STR: str = "/api/v1"
    PROJECT_NAME: str = "Student Service API"
    POSTGRES_URI: str = os.getenv("POSTGRES_URI", "postgresql://postgres:admin@localhost:5432/QuanLySinhVien")
    SERVER_HOST: str = os.getenv("SERVER_HOST", "localhost")
    SERVER_PORT: int = int(os.getenv("SERVER_PORT", "3000"))
    UVICORN_WORKERS: int = int(os.getenv("UVICORN_WORKERS", "1"))
    JWT_SECRET_KEY: str = os.getenv("JWT_SECRET_KEY", "tetdenxuanve")
    JWT_ALGORITHM: str = os.getenv("JWT_ALGORITHM", "HS256")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "30"))

    class Config:
        case_sensitive = True

# Initialize logging configuration
logging.config.dictConfig(LOGGING_CONFIG)

settings = Settings()