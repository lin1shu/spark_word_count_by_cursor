"""
Unit tests for the config module.
"""

import os
from unittest import mock

import pytest

from spark_word_count.config import DatabaseConfig, SparkConfig, WebConfig, AppConfig


def test_database_config_defaults():
    """Test that DatabaseConfig has the expected defaults."""
    config = DatabaseConfig()
    assert config.dbname == 'wordcount'
    assert config.user == 'postgres'
    assert config.password == 'sparkdemo'
    assert config.host == 'localhost'
    assert config.port == '5432'


def test_database_config_from_env():
    """Test that DatabaseConfig loads from environment variables."""
    with mock.patch.dict(os.environ, {'DB_NAME': 'testdb', 'DB_USER': 'testuser'}):
        config = DatabaseConfig.from_env()
        assert config.dbname == 'testdb'
        assert config.user == 'testuser'
        # Other fields should still have defaults
        assert config.password == 'sparkdemo'
        assert config.host == 'localhost'
        assert config.port == '5432'


def test_database_config_to_dict():
    """Test that to_dict converts the config to a dictionary."""
    config = DatabaseConfig(dbname='testdb', user='testuser')
    config_dict = config.to_dict()
    assert config_dict == {
        'dbname': 'testdb',
        'user': 'testuser',
        'password': 'sparkdemo',
        'host': 'localhost',
        'port': '5432',
    }


def test_database_config_to_jdbc_properties():
    """Test that to_jdbc_properties converts the config to JDBC properties."""
    config = DatabaseConfig(user='testuser', password='testpass')
    jdbc_props = config.to_jdbc_properties()
    assert jdbc_props == {
        'user': 'testuser',
        'password': 'testpass',
        'driver': 'org.postgresql.Driver',
    }


def test_database_config_get_jdbc_url():
    """Test that get_jdbc_url returns the correct JDBC URL."""
    config = DatabaseConfig(dbname='testdb', host='testhost', port='5433')
    jdbc_url = config.get_jdbc_url()
    assert jdbc_url == 'jdbc:postgresql://testhost:5433/testdb'


def test_spark_config_defaults():
    """Test that SparkConfig has the expected defaults."""
    config = SparkConfig()
    assert config.app_name == 'WordCount'
    assert config.driver_memory == '4g'
    assert config.executor_memory == '4g'
    assert config.max_result_size == '2g'
    assert config.shuffle_partitions == 10
    assert config.default_parallelism == 10


def test_web_config_defaults():
    """Test that WebConfig has the expected defaults."""
    config = WebConfig()
    assert config.host == '0.0.0.0'
    assert config.port == 5001
    assert config.debug is False


def test_app_config_integration():
    """Test that AppConfig integrates with all sub-configs."""
    db_config = DatabaseConfig(dbname='testdb')
    spark_config = SparkConfig(app_name='TestApp')
    web_config = WebConfig(port=8080)
    
    app_config = AppConfig(db_config, spark_config, web_config)
    
    assert app_config.db.dbname == 'testdb'
    assert app_config.spark.app_name == 'TestApp'
    assert app_config.web.port == 8080 