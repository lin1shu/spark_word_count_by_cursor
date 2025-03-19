"""
Custom exceptions for the Spark Word Count application.

This module defines application-specific exceptions that provide better context
and error handling capabilities throughout the application.
"""

class SparkWordCountError(Exception):
    """Base exception for all application errors."""
    pass


class SparkSessionError(SparkWordCountError):
    """Error creating or using Spark session."""
    pass


class DatabaseError(SparkWordCountError):
    """Base exception for all database-related errors."""
    pass


class DatabaseConnectionError(DatabaseError):
    """Error establishing a database connection."""
    pass


class DatabaseQueryError(DatabaseError):
    """Error executing a database query."""
    pass


class FileProcessingError(SparkWordCountError):
    """Error processing input or output files."""
    pass


class ConfigurationError(SparkWordCountError):
    """Error in application configuration."""
    pass


class WebAppError(SparkWordCountError):
    """Error in web application."""
    pass 