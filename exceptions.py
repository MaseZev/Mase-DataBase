"""
Custom exceptions for MaseDB client

Error Handling:
All API endpoints return appropriate HTTP status codes and error messages in case of failures.

Common Error Codes:
Status Code | Error Code | Description | How to Fix
400 | BAD_REQUEST | Invalid request parameters or malformed JSON | Check request body format and required fields
401 | UNAUTHORIZED | Missing or invalid API key | Include valid X-API-Key header
403 | FORBIDDEN | Insufficient permissions | Check API key permissions
404 | NOT_FOUND | Resource not found | Ensure collection/document exists
409 | CONFLICT | Resource already exists | Use unique identifiers
422 | VALIDATION_ERROR | Data validation error | Check data format and constraints
429 | RATE_LIMIT | Too many requests | Implement rate limiting
500 | INTERNAL_ERROR | Internal server error | Contact support
503 | SERVICE_UNAVAILABLE | Service temporarily unavailable | Retry later

Error Response Format:
All error responses follow this format:
{
    "error": {
        "code": "ERROR_CODE",
        "message": "Human readable error message",
        "details": {
            "field": "Additional error details if available"
        }
    }
}
"""

class MaseDBError(Exception):
    """
    Base exception for all MaseDB errors.
    
    Attributes:
        message (str): Human readable error message
        code (str): Error code from API
        details (dict): Additional error details if available
    """
    def __init__(self, message, code=None, details=None):
        self.message = message
        self.code = code
        self.details = details
        super().__init__(self.message)

class BadRequestError(MaseDBError):
    """
    400 - Bad Request
    
    Raised when request parameters are invalid or JSON is malformed.
    Check request body format and required fields.
    """
    pass

class UnauthorizedError(MaseDBError):
    """
    401 - Unauthorized
    
    Raised when API key is missing or invalid.
    Include valid X-API-Key header.
    """
    pass

class ForbiddenError(MaseDBError):
    """
    403 - Forbidden
    
    Raised when API key has insufficient permissions.
    Check API key permissions.
    """
    pass

class NotFoundError(MaseDBError):
    """
    404 - Not Found
    
    Raised when requested resource (collection/document) does not exist.
    Ensure collection/document exists.
    """
    pass

class ConflictError(MaseDBError):
    """
    409 - Conflict
    
    Raised when trying to create a resource that already exists.
    Use unique identifiers.
    """
    pass

class ValidationError(MaseDBError):
    """
    422 - Validation Error
    
    Raised when data validation fails.
    Check data format and constraints.
    """
    pass

class RateLimitError(MaseDBError):
    """
    429 - Rate Limit
    
    Raised when too many requests are made.
    Implement rate limiting.
    """
    pass

class InternalError(MaseDBError):
    """
    500 - Internal Server Error
    
    Raised when server encounters an internal error.
    Contact support.
    """
    pass

class ServiceUnavailableError(MaseDBError):
    """
    503 - Service Unavailable
    
    Raised when service is temporarily unavailable.
    Retry later.
    """
    pass

# Error code to exception mapping
ERROR_MAP = {
    'BAD_REQUEST': BadRequestError,
    'UNAUTHORIZED': UnauthorizedError,
    'FORBIDDEN': ForbiddenError,
    'NOT_FOUND': NotFoundError,
    'CONFLICT': ConflictError,
    'VALIDATION_ERROR': ValidationError,
    'RATE_LIMIT': RateLimitError,
    'INTERNAL_ERROR': InternalError,
    'SERVICE_UNAVAILABLE': ServiceUnavailableError
} 