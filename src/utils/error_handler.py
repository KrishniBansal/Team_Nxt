"""
error_handler.py - Centralized Error Handling for RetailPulse

This module provides:
- Custom exception classes for domain-specific errors
- Logging configuration
- Error handling decorators
- Validation utilities
"""

import logging
import functools
import os
import sys
from datetime import datetime

# ============================================
# LOGGING CONFIGURATION
# ============================================

LOG_DIR = os.path.join(os.path.dirname(__file__), "../../logs")
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, f"retailpulse_{datetime.now().strftime('%Y%m%d')}.log")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

def get_logger(name):
    """Get a logger instance for a module."""
    return logging.getLogger(name)


# ============================================
# CUSTOM EXCEPTION CLASSES
# ============================================

class RetailPulseError(Exception):
    """Base exception class for RetailPulse application."""

    def __init__(self, message, details=None):
        self.message = message
        self.details = details or {}
        super().__init__(self.message)

    def __str__(self):
        if self.details:
            return f"{self.message} | Details: {self.details}"
        return self.message


class DatabaseError(RetailPulseError):
    """Exception for database-related errors."""

    def __init__(self, message, table=None, operation=None):
        details = {}
        if table:
            details['table'] = table
        if operation:
            details['operation'] = operation
        super().__init__(message, details)


class DataValidationError(RetailPulseError):
    """Exception for data validation errors."""

    def __init__(self, message, field=None, value=None, reason=None):
        details = {}
        if field:
            details['field'] = field
        if value is not None:
            details['value'] = str(value)[:100]  # Truncate long values
        if reason:
            details['reason'] = reason
        super().__init__(message, details)


class FileError(RetailPulseError):
    """Exception for file-related errors."""

    def __init__(self, message, filepath=None, operation=None):
        details = {}
        if filepath:
            details['filepath'] = filepath
        if operation:
            details['operation'] = operation
        super().__init__(message, details)


class ConfigurationError(RetailPulseError):
    """Exception for configuration errors."""

    def __init__(self, message, config_key=None):
        details = {}
        if config_key:
            details['config_key'] = config_key
        super().__init__(message, details)


class ETLError(RetailPulseError):
    """Exception for ETL pipeline errors."""

    def __init__(self, message, stage=None, table=None, record_count=None):
        details = {}
        if stage:
            details['stage'] = stage
        if table:
            details['table'] = table
        if record_count is not None:
            details['record_count'] = record_count
        super().__init__(message, details)


class AnalyticsError(RetailPulseError):
    """Exception for analytics processing errors."""

    def __init__(self, message, analysis_type=None, customer_count=None):
        details = {}
        if analysis_type:
            details['analysis_type'] = analysis_type
        if customer_count is not None:
            details['customer_count'] = customer_count
        super().__init__(message, details)


# ============================================
# ERROR HANDLING DECORATORS
# ============================================

def handle_exceptions(logger=None, reraise=True, default_return=None):
    """
    Decorator to handle exceptions in functions.

    Args:
        logger: Logger instance for logging errors
        reraise: Whether to re-raise exceptions after logging
        default_return: Value to return if exception occurs and reraise=False
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            func_logger = logger or get_logger(func.__module__)
            try:
                return func(*args, **kwargs)
            except RetailPulseError as e:
                func_logger.error(f"RetailPulse error in {func.__name__}: {e}")
                if reraise:
                    raise
                return default_return
            except Exception as e:
                func_logger.exception(f"Unexpected error in {func.__name__}: {e}")
                if reraise:
                    raise
                return default_return
        return wrapper
    return decorator


def retry_on_error(max_retries=3, delay=1, exceptions=(Exception,)):
    """
    Decorator to retry function on specific exceptions.

    Args:
        max_retries: Maximum number of retry attempts
        delay: Delay in seconds between retries
        exceptions: Tuple of exception types to catch
    """
    import time

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = get_logger(func.__module__)
            last_exception = None

            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        logger.warning(
                            f"Attempt {attempt + 1}/{max_retries} failed for {func.__name__}: {e}. "
                            f"Retrying in {delay}s..."
                        )
                        time.sleep(delay)
                    else:
                        logger.error(f"All {max_retries} attempts failed for {func.__name__}")

            raise last_exception
        return wrapper
    return decorator


# ============================================
# VALIDATION UTILITIES
# ============================================

def validate_file_exists(filepath, error_msg=None):
    """
    Validate that a file exists.

    Args:
        filepath: Path to the file
        error_msg: Custom error message

    Raises:
        FileError: If file doesn't exist
    """
    if not os.path.exists(filepath):
        msg = error_msg or f"File not found: {filepath}"
        raise FileError(msg, filepath=filepath, operation='read')


def validate_directory_exists(dirpath, create=True):
    """
    Validate that a directory exists, optionally create it.

    Args:
        dirpath: Path to the directory
        create: Whether to create the directory if it doesn't exist

    Raises:
        FileError: If directory doesn't exist and create=False
    """
    if not os.path.exists(dirpath):
        if create:
            os.makedirs(dirpath, exist_ok=True)
        else:
            raise FileError(f"Directory not found: {dirpath}", filepath=dirpath)


def validate_not_empty(value, field_name):
    """
    Validate that a value is not empty.

    Args:
        value: Value to check
        field_name: Name of the field for error message

    Raises:
        DataValidationError: If value is empty
    """
    if value is None or (isinstance(value, str) and not value.strip()):
        raise DataValidationError(
            f"Value cannot be empty",
            field=field_name,
            value=value,
            reason="empty_value"
        )


def validate_positive_number(value, field_name):
    """
    Validate that a value is a positive number.

    Args:
        value: Value to check
        field_name: Name of the field for error message

    Raises:
        DataValidationError: If value is not a positive number
    """
    try:
        num = float(value)
        if num < 0:
            raise DataValidationError(
                f"Value must be non-negative",
                field=field_name,
                value=value,
                reason="negative_value"
            )
    except (TypeError, ValueError):
        raise DataValidationError(
            f"Value must be a number",
            field=field_name,
            value=value,
            reason="invalid_number"
        )


def validate_database_connection(conn):
    """
    Validate that a database connection is valid.

    Args:
        conn: Database connection object

    Raises:
        DatabaseError: If connection is invalid
    """
    if conn is None:
        raise DatabaseError("Database connection is None", operation='validate')

    try:
        conn.execute("SELECT 1")
    except Exception as e:
        raise DatabaseError(f"Database connection test failed: {e}", operation='validate')


# ============================================
# CONTEXT MANAGERS
# ============================================

class DatabaseConnection:
    """Context manager for database connections with error handling."""

    def __init__(self, db_path, logger=None):
        self.db_path = db_path
        self.logger = logger or get_logger(__name__)
        self.conn = None

    def __enter__(self):
        import sqlite3
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
            self.logger.debug(f"Connected to database: {self.db_path}")
            return self.conn
        except sqlite3.Error as e:
            self.logger.error(f"Failed to connect to database: {e}")
            raise DatabaseError(f"Failed to connect to database: {e}", operation='connect')

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            try:
                if exc_type is None:
                    self.conn.commit()
                else:
                    self.conn.rollback()
                    self.logger.warning("Transaction rolled back due to error")
            except Exception as e:
                self.logger.error(f"Error during connection cleanup: {e}")
            finally:
                self.conn.close()
                self.logger.debug("Database connection closed")

        # Don't suppress exceptions
        return False


# ============================================
# ERROR REPORTING
# ============================================

def format_error_report(errors):
    """
    Format a list of errors into a readable report.

    Args:
        errors: List of error dictionaries or exception objects

    Returns:
        str: Formatted error report
    """
    if not errors:
        return "No errors recorded."

    report_lines = [
        "=" * 60,
        "ERROR REPORT",
        f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Total Errors: {len(errors)}",
        "=" * 60,
        ""
    ]

    for i, error in enumerate(errors, 1):
        if isinstance(error, dict):
            report_lines.append(f"Error {i}:")
            for key, value in error.items():
                report_lines.append(f"  {key}: {value}")
        elif isinstance(error, Exception):
            report_lines.append(f"Error {i}: {type(error).__name__}")
            report_lines.append(f"  Message: {str(error)}")
        else:
            report_lines.append(f"Error {i}: {str(error)}")
        report_lines.append("")

    return "\n".join(report_lines)
