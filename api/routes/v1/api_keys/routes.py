"""
REST API routes for API key management.

Users can create, list, and revoke their own API keys.
All routes require Firebase authentication.
"""

from datetime import datetime, timedelta, timezone
from typing import List

from fastapi import APIRouter, Depends, HTTPException

from api.security.deps import AuthContext, current_auth
from api.routes.v1.api_keys.dto import (
    CreateApiKeyRequest,
    CreateApiKeyResponse,
    ApiKeyDTO,
    RevokeApiKeyResponse,
)

router = APIRouter(prefix="/api-keys", tags=["api-keys"])

# The service will be injected via dependency
_api_key_service = None


def set_api_key_service(service):
    """Set the API key service (called during app startup)."""
    global _api_key_service
    _api_key_service = service


def get_api_key_service():
    """Get the API key service."""
    if _api_key_service is None:
        raise HTTPException(
            status_code=503,
            detail="API key service not initialized"
        )
    return _api_key_service


@router.post(
    "/",
    response_model=CreateApiKeyResponse,
    summary="Create a new API key",
    description="""
    Create a new API key for your account.
    
    **IMPORTANT**: The full API key is only returned ONCE in this response.
    You must save it immediately - it cannot be retrieved later.
    
    If you lose your key, you'll need to create a new one.
    """
)
async def create_api_key(
    request: CreateApiKeyRequest,
    auth: AuthContext = Depends(current_auth),
):
    """Create a new API key for the authenticated user."""
    service = get_api_key_service()
    
    # Calculate expiration if specified
    expires_at = None
    if request.expires_in_days:
        expires_at = datetime.now(timezone.utc) + timedelta(days=request.expires_in_days)
    
    result = await service.create_key(
        account_id=auth.account_id,
        name=request.name,
        expires_at=expires_at,
    )
    
    return CreateApiKeyResponse(
        id=result.api_key_id,
        key_prefix=result.key_prefix,
        full_key=result.full_key,
        name=result.name,
        created_at=result.created_at,
        expires_at=expires_at,
    )


@router.get(
    "/",
    response_model=List[ApiKeyDTO],
    summary="List your API keys",
    description="List all API keys for your account (active and revoked)."
)
async def list_api_keys(
    auth: AuthContext = Depends(current_auth),
):
    """List all API keys for the authenticated user."""
    service = get_api_key_service()
    
    keys = await service.list_keys(auth.account_id)
    
    return [
        ApiKeyDTO(
            id=key["id"],
            key_prefix=key["key_prefix"],
            name=key["name"],
            is_active=key["is_active"],
            created_at=datetime.fromisoformat(key["created_at"]),
            last_used_at=datetime.fromisoformat(key["last_used_at"]) if key["last_used_at"] else None,
            expires_at=datetime.fromisoformat(key["expires_at"]) if key["expires_at"] else None,
        )
        for key in keys
    ]


@router.delete(
    "/{api_key_id}",
    response_model=RevokeApiKeyResponse,
    summary="Revoke an API key",
    description="""
    Revoke (deactivate) an API key. This is a soft delete - the key
    will no longer work, but it will still appear in your key list
    with is_active=false.
    
    Revoked keys cannot be reactivated. Create a new key instead.
    """
)
async def revoke_api_key(
    api_key_id: int,
    auth: AuthContext = Depends(current_auth),
):
    """Revoke an API key owned by the authenticated user."""
    service = get_api_key_service()
    
    success = await service.revoke_key(
        api_key_id=api_key_id,
        account_id=auth.account_id,
    )
    
    if not success:
        raise HTTPException(
            status_code=404,
            detail="API key not found or not owned by you"
        )
    
    return RevokeApiKeyResponse(
        success=True,
        message=f"API key {api_key_id} has been revoked"
    )
