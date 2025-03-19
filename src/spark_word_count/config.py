"""
Configuration management for Spark Word Count.

This module handles loading configuration from environment variables,
configuration files, and provides defaults.
"""

import os
from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class DatabaseConfig:
    """Database connection configuration."""

    dbname: str = "wordcount"
    user: str = "postgres"
    password: str = "sparkdemo"
    host: str = "localhost"
    port: str = "5432"

    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        """Create database config from environment variables."""
        return cls(
            dbname=os.environ.get("DB_NAME", cls.dbname),
            user=os.environ.get("DB_USER", cls.user),
            password=os.environ.get("DB_PASSWORD", cls.password),
            host=os.environ.get("DB_HOST", cls.host),
            port=os.environ.get("DB_PORT", cls.port),
        )

    def to_dict(self) -> Dict[str, str]:
        """Convert to dictionary for psycopg2."""
        return {
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
            "host": self.host,
            "port": self.port,
        }

    def to_jdbc_properties(self) -> Dict[str, str]:
        """Convert to JDBC connection properties for PySpark."""
        return {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver",
        }

    def get_jdbc_url(self) -> str:
        """Get JDBC URL for this configuration."""
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.dbname}"


@dataclass
class SparkConfig:
    """Spark configuration."""

    app_name: str = "WordCount"
    driver_memory: str = "4g"
    executor_memory: str = "4g"
    max_result_size: str = "2g"
    shuffle_partitions: int = 10
    default_parallelism: int = 10

    @classmethod
    def from_env(cls) -> "SparkConfig":
        """Create Spark config from environment variables."""
        return cls(
            app_name=os.environ.get("SPARK_APP_NAME", cls.app_name),
            driver_memory=os.environ.get("SPARK_DRIVER_MEMORY", cls.driver_memory),
            executor_memory=os.environ.get("SPARK_EXECUTOR_MEMORY", cls.executor_memory),
            max_result_size=os.environ.get("SPARK_MAX_RESULT_SIZE", cls.max_result_size),
            shuffle_partitions=int(
                os.environ.get("SPARK_SHUFFLE_PARTITIONS", cls.shuffle_partitions)
            ),
            default_parallelism=int(
                os.environ.get("SPARK_DEFAULT_PARALLELISM", cls.default_parallelism)
            ),
        )


@dataclass
class WebConfig:
    """Web application configuration."""

    host: str = "0.0.0.0"
    port: int = 5001
    debug: bool = False

    @classmethod
    def from_env(cls) -> "WebConfig":
        """Create web config from environment variables."""
        return cls(
            host=os.environ.get("WEB_HOST", cls.host),
            port=int(os.environ.get("WEB_PORT", cls.port)),
            debug=os.environ.get("WEB_DEBUG", "").lower() in ("true", "1", "t"),
        )


class AppConfig:
    """Main application configuration."""

    def __init__(
        self,
        db_config: Optional[DatabaseConfig] = None,
        spark_config: Optional[SparkConfig] = None,
        web_config: Optional[WebConfig] = None,
    ):
        """Initialize application configuration."""
        self.db = db_config or DatabaseConfig.from_env()
        self.spark = spark_config or SparkConfig.from_env()
        self.web = web_config or WebConfig.from_env()


# Default application configuration
config = AppConfig()
