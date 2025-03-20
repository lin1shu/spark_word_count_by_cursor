"""
Configuration management for Spark Word Count.

This module handles loading configuration from environment variables,
configuration files, and provides defaults.
"""

import json
import os
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Optional


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
        return asdict(self)

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


@dataclass
class AppConfig:
    """Main application configuration."""

    db: DatabaseConfig = field(default_factory=DatabaseConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)
    web: WebConfig = field(default_factory=WebConfig)


def get_config(config_file: Optional[str] = None) -> Dict[str, Any]:
    """
    Get configuration from a file or default values.

    Args:
        config_file: Path to the configuration file (default: config.json in the current directory)

    Returns:
        Dict[str, Any]: Configuration dictionary
    """
    if config_file is None:
        config_file = os.path.join(os.getcwd(), "config.json")

    # Default configuration
    config: Dict[str, Any] = {
        "db": DatabaseConfig().to_dict(),
        "spark": asdict(SparkConfig()),
        "web": asdict(WebConfig()),
    }

    # Load configuration from file if it exists
    if os.path.exists(config_file):
        try:
            with open(config_file, "r") as f:
                file_config = json.load(f)
                # Update the default configuration with values from the file
                for section, values in file_config.items():
                    if section in config and isinstance(values, dict):
                        config[section].update(values)
        except Exception as e:
            print(f"Error loading configuration from {config_file}: {e}")

    return config


def get_db_config() -> Dict[str, Any]:
    """
    Get database configuration with environment variable overrides.

    Returns:
        Dict[str, Any]: Database configuration dictionary
    """
    # Create default config based on DatabaseConfig
    db_config = {
        "host": "localhost",
        "port": "5432",
        "dbname": "wordcount",
        "user": "postgres",
        "password": "sparkdemo",
    }

    # Override with environment variables if set
    env_mappings = {
        "DB_HOST": "host",
        "DB_PORT": "port",
        "DB_NAME": "dbname",
        "DB_USER": "user",
        "DB_PASSWORD": "password",
    }

    for env_var, config_key in env_mappings.items():
        if env_var in os.environ:
            db_config[config_key] = os.environ[env_var]

    return db_config


# Default application configuration
config = AppConfig()
