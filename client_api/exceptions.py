"""
Custom exceptions for the QuantX client SDK.
"""


class QuantXError(Exception):
    """Base exception for all QuantX client errors."""
    
    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class AuthenticationError(QuantXError):
    """Raised when API key authentication fails."""
    
    def __init__(self, message: str = "Authentication failed"):
        super().__init__(message)


class ConnectionError(QuantXError):
    """Raised when WebSocket connection fails."""
    
    def __init__(self, message: str = "Connection failed"):
        super().__init__(message)


class NotConnectedError(QuantXError):
    """Raised when attempting operations without an active connection."""
    
    def __init__(self, message: str = "Not connected. Call connect() first."):
        super().__init__(message)


class OrderError(QuantXError):
    """Raised when order submission or cancellation fails."""
    
    def __init__(self, message: str, error_type: str = "ORDER_ERROR"):
        self.error_type = error_type
        super().__init__(message)

