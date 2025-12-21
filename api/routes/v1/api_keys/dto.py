"""
DTOs for API Key routes.
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class CreateApiKeyRequest(BaseModel):
    """Request to create a new API key."""
    name: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="A label to identify this API key"
    )
    expires_in_days: Optional[int] = Field(
        None,
        ge=1,
        le=365,
        description="Optional: Number of days until the key expires"
    )


class CreateApiKeyResponse(BaseModel):
    """
    Response when creating a new API key.
    
    IMPORTANT: The full_key is only returned ONCE at creation time.
    Users must save it immediately - it cannot be retrieved later.
    """
    id: int
    key_prefix: str
    full_key: str  # Only shown once!
    name: str
    created_at: datetime
    expires_at: Optional[datetime] = None
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class ApiKeyDTO(BaseModel):
    """API key info (without the actual key)."""
    id: int
    key_prefix: str
    name: str
    is_active: bool
    created_at: datetime
    last_used_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }


class RevokeApiKeyResponse(BaseModel):
    """Response when revoking an API key."""
    success: bool
    message: str
