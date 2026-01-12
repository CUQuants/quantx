from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
import os
import dotenv

from models import Base

# Try to get DATABASE_URL from environment (Docker sets this)
# Fall back to .env file for local development
DATABASE_URL = os.getenv("DATABASE_URL") or dotenv.get_key(".env", "DATABASE_URL")

if not DATABASE_URL:
    raise ValueError(
        "DATABASE_URL not found. Set it as an environment variable or in .env file. "
        "For Docker: DATABASE_URL should be set in docker-compose.yml. "
        "For local dev: Create a .env file with DATABASE_URL."
    )

engine = create_async_engine(DATABASE_URL, echo=False, future=True)
SessionFactory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

async def init_models():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def get_session() -> AsyncSession:
    async with SessionFactory() as session:
        yield session