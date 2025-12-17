from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
import dotenv

from models import Base

DATABASE_URL = dotenv.get_key(".env", "DATABASE_URL")

engine = create_async_engine(DATABASE_URL, echo=False, future=True)
SessionFactory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

async def init_models():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def get_session() -> AsyncSession:
    async with SessionFactory() as session:
        yield session