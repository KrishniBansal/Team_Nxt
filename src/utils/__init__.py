"""
RetailPulse Utilities Package

This package contains shared utilities for the RetailPulse application.
"""

from .error_handler import (
    # Logging
    get_logger,

    # Exceptions
    RetailPulseError,
    DatabaseError,
    DataValidationError,
    FileError,
    ConfigurationError,
    ETLError,
    AnalyticsError,

    # Decorators
    handle_exceptions,
    retry_on_error,

    # Validation
    validate_file_exists,
    validate_directory_exists,
    validate_not_empty,
    validate_positive_number,
    validate_database_connection,

    # Context Managers
    DatabaseConnection,

    # Reporting
    format_error_report,
)

__all__ = [
    'get_logger',
    'RetailPulseError',
    'DatabaseError',
    'DataValidationError',
    'FileError',
    'ConfigurationError',
    'ETLError',
    'AnalyticsError',
    'handle_exceptions',
    'retry_on_error',
    'validate_file_exists',
    'validate_directory_exists',
    'validate_not_empty',
    'validate_positive_number',
    'validate_database_connection',
    'DatabaseConnection',
    'format_error_report',
]
