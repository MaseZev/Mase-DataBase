"""
MaseDB - Python client library for Mase Database API

Base URL: https://masedb.maseai.online

Authentication:
All API endpoints require authentication using API keys.

API Key Authentication:
Include the API key in the X-API-Key header:
X-API-Key: <your_api_key>

Features:
- MongoDB-style query operators
- Transaction support
- Index management
- Statistics and monitoring
- Comprehensive error handling
- Async client support

Example:
    >>> from masedb import MaseDBClient
    >>> client = MaseDBClient(api_key="your_api_key")
    >>> # Create a collection
    >>> client.create_collection("users", "User collection")
    >>> # Insert a document
    >>> client.insert_one("users", {"name": "John", "age": 30})
    >>> # Query documents
    >>> users = client.list_documents("users", {"age": {"$gt": 25}})
"""

__version__ = "0.1.1"

from .client import MaseDBClient
from .async_client import AsyncMaseDBClient
from .exceptions import (
    MaseDBError,
    BadRequestError,
    UnauthorizedError,
    ForbiddenError,
    NotFoundError,
    ConflictError,
    ValidationError,
    RateLimitError,
    InternalError,
    ServiceUnavailableError
)

__all__ = [
    'MaseDBClient',
    'AsyncMaseDBClient',
    'MaseDBError',
    'BadRequestError',
    'UnauthorizedError',
    'ForbiddenError',
    'NotFoundError',
    'ConflictError',
    'ValidationError',
    'RateLimitError',
    'InternalError',
    'ServiceUnavailableError'
] 