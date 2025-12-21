"""
API Key model for programmatic API authentication.

API keys are used by trading bots and external applications to authenticate
with the WebSocket API without using Firebase tokens.
"""

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import Integer, String, DateTime, Boolean, ForeignKey, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship

from . import Base


class ApiKey(Base):
    """
    API Key for programmatic access to the trading API.
    
    The key itself is hashed before storage (like a password).
    Only the prefix is stored in plaintext for identification.
    """
    __tablename__ = "api_keys"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    
    # The hashed API key (using SHA-256)
    # The actual key is only shown once at creation time
    key_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True, nullable=False)
    
    # Prefix for display/identification (e.g., "qntx_live_a1b2c3d4")
    # Stored in plaintext so users can identify their keys
    key_prefix: Mapped[str] = mapped_column(String(20), nullable=False)
    
    # User-provided label for this key
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    
    # The account this key belongs to
    account_id: Mapped[int] = mapped_column(
        Integer, 
        ForeignKey("accounts.id", ondelete="CASCADE"), 
        nullable=False,
        index=True
    )
    
    # Soft delete / revocation flag
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime, 
        default=lambda: datetime.now(timezone.utc),
        nullable=False
    )
    
    last_used_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, 
        nullable=True
    )
    
    # Optional expiration (null = never expires)
    expires_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, 
        nullable=True
    )
    
    # Relationship to Account
    account: Mapped["Account"] = relationship(
        "Account",
        back_populates="api_keys",
        lazy="selectin"
    )

    def is_expired(self) -> bool:
        """Check if the API key has expired."""
        if self.expires_at is None:
            return False
        return datetime.now(timezone.utc) > self.expires_at

    def is_valid(self) -> bool:
        """Check if the API key is valid (active and not expired)."""
        return self.is_active and not self.is_expired()


# Index for efficient lookups
Index("ix_api_keys_account_active", ApiKey.account_id, ApiKey.is_active)
