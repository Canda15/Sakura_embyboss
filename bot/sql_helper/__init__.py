"""
初始化数据库
"""
from bot import db_host, db_user, db_pwd, db_name, db_port
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base

# 创建engine对象 (使用 asyncmy 作为驱动)
engine = create_async_engine(
    f"mysql+asyncmy://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}?charset=utf8mb4",
    echo=False,
    echo_pool=False,
    pool_size=16,
    pool_recycle=60 * 30,
    connect_args={"init_command": "SET NAMES utf8mb4"},
)

# 创建Base对象
Base = declarative_base()

# 创建全局异步Session工厂
Session = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False
)

async def init_db():
    """初始化数据库表，需要在程序启动的异步事件/入口处调用"""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all, checkfirst=True)
