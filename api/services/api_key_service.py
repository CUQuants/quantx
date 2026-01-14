"""
API Key Service for the QuantX trading system.

Responsibilities:
    - Generate new API keys for accounts
    - Validate API keys and return associated account info
    - Cache validated keys in memory for fast lookups
    - Revoke API keys

Security:
    - API keys are hashed using SHA-256 before storage
    - Full key is only returned once at creation time
    - Cache has TTL to allow revocation to take effect
"""

import asyncio
import hashlib
import logging
import secrets
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from models import Account, ApiKey

logger = logging.getLogger(__name__)

# Key format: qntx_live_<32 random hex chars>
KEY_PREFIX = "qntx_live_"
KEY_RANDOM_BYTES = 16  # 32 hex chars


@dataclass
class ApiKeyInfo:
    """Cached information about a validated API key."""
    account_id: int
    user_id: str  # Firebase UID (for compatibility with existing WebSocketClient)
    username: str
    api_key_id: int
    validated_at: datetime


@dataclass
class ApiKeyCreateResult:
    """Result of creating a new API key."""
    api_key_id: int
    key_prefix: str
    full_key: str  # Only returned once at creation!
    name: str
    created_at: datetime


class ApiKeyService:
    """
    Service for API key management and validation.
    
    Provides in-memory caching of validated keys to avoid
    database lookups on every request.
    """

    def __init__(
        self,
        session_factory,
        cache_ttl_seconds: int = 300,  # 5 minutes
    ):
        """
        Initialize the API key service.
        
        Args:
            session_factory: Async session factory for DB operations
            cache_ttl_seconds: How long to cache validated keys
        """
        self._session_factory = session_factory
        self._cache_ttl = cache_ttl_seconds
        
        # Cache: key_hash -> (ApiKeyInfo, cached_at)
        self._cache: Dict[str, Tuple[ApiKeyInfo, datetime]] = {}
        self._cache_lock = asyncio.Lock()
        
        # Statistics
        self._cache_hits = 0
        self._cache_misses = 0
        self._validations = 0
        
        self._logger = logging.getLogger("quantx.services.ApiKeyService")

    # =========================================================================
    # Key Generation
    # =========================================================================

    @staticmethod
    def _generate_key() -> str:
        """Generate a new random API key."""
        random_part = secrets.token_hex(KEY_RANDOM_BYTES)
        return f"{KEY_PREFIX}{random_part}"

    @staticmethod
    def _hash_key(key: str) -> str:
        """Hash an API key using SHA-256."""
        return hashlib.sha256(key.encode()).hexdigest()

    @staticmethod
    def _get_key_prefix(key: str) -> str:
        """Extract the display prefix from a full key."""
        # Show "qntx_live_" + first 8 chars of random part
        return key[:len(KEY_PREFIX) + 8]

    # =========================================================================
    # Key Management
    # =========================================================================

    async def create_key(
        self,
        account_id: int,
        name: str,
        expires_at: Optional[datetime] = None,
    ) -> ApiKeyCreateResult:
        """
        Create a new API key for an account.
        
        Args:
            account_id: The account to create the key for
            name: User-provided label for this key
            expires_at: Optional expiration datetime
            
        Returns:
            ApiKeyCreateResult with the full key (only shown once!)
        """
        full_key = self._generate_key()
        key_hash = self._hash_key(full_key)
        key_prefix = self._get_key_prefix(full_key)
        
        async with self._session_factory() as session:
            async with session.begin():
                api_key = ApiKey(
                    key_hash=key_hash,
                    key_prefix=key_prefix,
                    name=name,
                    account_id=account_id,
                    is_active=True,
                    created_at=datetime.now(timezone.utc),
                    expires_at=expires_at,
                )
                session.add(api_key)
                await session.flush()
                await session.refresh(api_key)
                
                api_key_id = api_key.id
                created_at = api_key.created_at
        
        self._logger.info(
            f"Created API key {key_prefix}... for account {account_id}"
        )
        
        return ApiKeyCreateResult(
            api_key_id=api_key_id,
            key_prefix=key_prefix,
            full_key=full_key,
            name=name,
            created_at=created_at,
        )

    async def revoke_key(self, api_key_id: int, account_id: int) -> bool:
        """
        Revoke an API key (soft delete).
        
        Args:
            api_key_id: The key ID to revoke
            account_id: The account ID (for ownership validation)
            
        Returns:
            True if revoked, False if not found or not owned
        """
        async with self._session_factory() as session:
            async with session.begin():
                result = await session.execute(
                    select(ApiKey)
                    .where(ApiKey.id == api_key_id)
                    .where(ApiKey.account_id == account_id)
                )
                api_key = result.scalar_one_or_none()
                
                if not api_key:
                    return False
                
                api_key.is_active = False
                await session.flush()
        
        # Invalidate cache for this key
        await self._invalidate_cache_by_id(api_key_id)
        
        self._logger.info(f"Revoked API key {api_key_id} for account {account_id}")
        
        return True

    async def list_keys(self, account_id: int) -> List[dict]:
        """
        List all API keys for an account.
        
        Returns:
            List of key info dicts (without the actual key hash)
        """
        async with self._session_factory() as session:
            result = await session.execute(
                select(ApiKey)
                .where(ApiKey.account_id == account_id)
                .order_by(ApiKey.created_at.desc())
            )
            keys = result.scalars().all()
            
            return [
                {
                    "id": key.id,
                    "key_prefix": key.key_prefix,
                    "name": key.name,
                    "is_active": key.is_active,
                    "created_at": key.created_at.isoformat(),
                    "last_used_at": key.last_used_at.isoformat() if key.last_used_at else None,
                    "expires_at": key.expires_at.isoformat() if key.expires_at else None,
                }
                for key in keys
            ]

    # =========================================================================
    # Key Validation
    # =========================================================================

    async def validate_key(self, api_key: str) -> Optional[ApiKeyInfo]:
        """
        Validate an API key and return the associated account info.
        
        Uses in-memory cache to avoid DB lookups on every request.
        
        Args:
            api_key: The full API key to validate
            
        Returns:
            ApiKeyInfo if valid, None if invalid
        """
        self._validations += 1
        
        # Hash the key for lookup
        key_hash = self._hash_key(api_key)
        
        # Check cache first
        cached = await self._get_from_cache(key_hash)
        if cached is not None:
            self._cache_hits += 1
            return cached
        
        self._cache_misses += 1
        
        # Not in cache, validate against DB
        info = await self._validate_against_db(key_hash)
        
        if info:
            await self._add_to_cache(key_hash, info)
            # Update last_used_at asynchronously (don't block)
            asyncio.create_task(self._update_last_used(info.api_key_id))
        
        return info

    async def _validate_against_db(self, key_hash: str) -> Optional[ApiKeyInfo]:
        """Validate a key hash against the database."""
        async with self._session_factory() as session:
            result = await session.execute(
                select(ApiKey, Account)
                .join(Account, ApiKey.account_id == Account.id)
                .where(ApiKey.key_hash == key_hash)
                .where(ApiKey.is_active == True)
            )
            row = result.first()
            
            if not row:
                self._logger.debug(f"API key not found or inactive")
                return None
            
            api_key, account = row
            
            # Check expiration
            if api_key.is_expired():
                self._logger.debug(f"API key {api_key.id} has expired")
                return None
            
            return ApiKeyInfo(
                account_id=account.id,
                user_id=account.firebase_uid or f"api_user_{account.id}",
                username=account.username,
                api_key_id=api_key.id,
                validated_at=datetime.now(timezone.utc),
            )

    async def _update_last_used(self, api_key_id: int) -> None:
        """Update the last_used_at timestamp (async, non-blocking)."""
        try:
            async with self._session_factory() as session:
                async with session.begin():
                    await session.execute(
                        update(ApiKey)
                        .where(ApiKey.id == api_key_id)
                        .values(last_used_at=datetime.now(timezone.utc))
                    )
        except Exception as e:
            self._logger.warning(f"Failed to update last_used_at: {e}")

    # =========================================================================
    # Cache Management
    # =========================================================================

    async def _get_from_cache(self, key_hash: str) -> Optional[ApiKeyInfo]:
        """Get a validated key from cache if still valid."""
        async with self._cache_lock:
            if key_hash not in self._cache:
                return None
            
            info, cached_at = self._cache[key_hash]
            
            # Check TTL
            age = (datetime.now(timezone.utc) - cached_at).total_seconds()
            if age > self._cache_ttl:
                del self._cache[key_hash]
                return None
            
            return info

    async def _add_to_cache(self, key_hash: str, info: ApiKeyInfo) -> None:
        """Add a validated key to the cache."""
        async with self._cache_lock:
            self._cache[key_hash] = (info, datetime.now(timezone.utc))

    async def _invalidate_cache_by_id(self, api_key_id: int) -> None:
        """Remove a key from cache by its ID (used when revoking)."""
        async with self._cache_lock:
            # Find and remove any cached entry with this ID
            to_remove = [
                key_hash for key_hash, (info, _) in self._cache.items()
                if info.api_key_id == api_key_id
            ]
            for key_hash in to_remove:
                del self._cache[key_hash]

    async def clear_cache(self) -> None:
        """Clear the entire cache."""
        async with self._cache_lock:
            self._cache.clear()

    # =========================================================================
    # Health Check
    # =========================================================================

    async def health_check(self) -> dict:
        """Return health metrics for the API key service."""
        async with self._cache_lock:
            cache_size = len(self._cache)
        
        hit_rate = (
            self._cache_hits / self._validations * 100
            if self._validations > 0 else 0
        )
        
        return {
            "service": "ApiKeyService",
            "cache_size": cache_size,
            "cache_ttl_seconds": self._cache_ttl,
            "total_validations": self._validations,
            "cache_hits": self._cache_hits,
            "cache_misses": self._cache_misses,
            "cache_hit_rate_percent": round(hit_rate, 2),
        }
