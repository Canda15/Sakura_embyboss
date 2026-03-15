"""
基本的sql操作
"""
from sqlalchemy import Column, BigInteger, String, DateTime, Integer, case
from sqlalchemy import func
from sqlalchemy import or_, select, update, delete
from bot.sql_helper import Base, Session, engine
from bot import LOGGER

class Emby(Base):
    """
    emby表，tg主键，默认值lv，us，iv
    """
    __tablename__ = 'emby'
    tg = Column(BigInteger, primary_key=True, autoincrement=False)
    embyid = Column(String(255), nullable=True)
    name = Column(String(255), nullable=True)
    pwd = Column(String(255), nullable=True)
    pwd2 = Column(String(255), nullable=True)
    lv = Column(String(1), default='d')
    cr = Column(DateTime, nullable=True)
    ex = Column(DateTime, nullable=True)
    us = Column(Integer, default=0)
    iv = Column(Integer, default=0)
    ch = Column(DateTime, nullable=True)

# 异步模式下不能直接调用 Emby.__table__.create(bind=engine, checkfirst=True)
# 被移入 init_db 中统一初始化
# Emby.__table__.create(bind=engine, checkfirst=True)

async def sql_add_emby(tg: int):
    """
    添加一条emby记录，如果tg已存在则忽略
    """
    async with Session() as session:
        async with session.begin():
            try:
                emby = Emby(tg=tg)
                session.add(emby)
                await session.commit()
            except:
                pass

async def sql_delete_emby_by_tg(tg):
    """
    根据tg删除一条emby记录
    """
    async with Session() as session:
        try:
            result = await session.execute(select(Emby).filter(Emby.tg == tg))
            emby = result.scalars().first()
            if emby:
                await session.delete(emby)
                await session.commit()
                LOGGER.info(f"删除数据库记录成功 {tg}")
                return True
            else:
                LOGGER.info(f"数据库记录不存在 {tg}")
                return False
        except Exception as e:
            LOGGER.error(f"删除数据库记录时发生异常 {e}")
            await session.rollback()
            return False

async def sql_clear_emby_iv():
    """
    清除所有emby的iv
    """
    async with Session() as session:
        try:
            await session.execute(update(Emby).values(iv=0))
            await session.commit()
            return True
        except Exception as e:
            LOGGER.error(f"清除所有emby的iv时发生异常 {e}")
            await session.rollback()
            return False

async def sql_delete_emby(tg=None, embyid=None, name=None):
    """
    根据tg, embyid或name删除一条emby记录
    至少需要提供一个参数，如果所有参数都为None，则返回False
    """
    async with Session() as session:
        try:
            # 构建条件列表，只包含非None的参数
            conditions = []
            if tg is not None:
                conditions.append(Emby.tg == tg)
            if embyid is not None:
                conditions.append(Emby.embyid == embyid)
            if name is not None:
                conditions.append(Emby.name == name)
            
            # 如果所有参数都为None，返回False
            if not conditions:
                LOGGER.warning("sql_delete_emby: 所有参数都为None，无法删除记录")
                return False
            
            # 使用or_组合所有条件
            condition = or_(*conditions)
            LOGGER.debug(f"删除数据库记录，条件: tg={tg}, embyid={embyid}, name={name}")
            
            result = await session.execute(select(Emby).filter(condition).with_for_update())
            emby = result.scalars().first()
            if emby:
                LOGGER.info(f"删除数据库记录 {emby.name} - {emby.embyid} - {emby.tg}")
                await session.delete(emby)
                try:
                    await session.commit()
                    LOGGER.info(f"成功删除数据库记录: tg={tg}, embyid={embyid}, name={name}")
                    return True
                except Exception as e:
                    LOGGER.error(f"删除数据库记录时提交事务失败 {e}")
                    await session.rollback()
                    return False
            else:
                LOGGER.info(f"数据库记录不存在: tg={tg}, embyid={embyid}, name={name}")
                return False
        except Exception as e:
            LOGGER.error(f"删除数据库记录时发生异常 {e}")
            await session.rollback()
            return False

async def sql_update_embys(some_list: list, method=None):
    """ 根据list中的tg值批量更新一些值 """
    async with Session() as session:
        if method == 'iv':
            try:
                for c in some_list:
                    await session.execute(update(Emby).where(Emby.tg == c[0]).values(iv=c[1]))
                await session.commit()
                return True
            except:
                await session.rollback()
                return False
        if method == 'ex':
            try:
                for c in some_list:
                    await session.execute(update(Emby).where(Emby.tg == c[0]).values(ex=c[1]))
                await session.commit()
                return True
            except:
                await session.rollback()
                return False
        if method == 'bind':
            try:
                for c in some_list:
                    await session.execute(update(Emby).where(Emby.tg == c[0]).values(name=c[1], embyid=c[2]))
                await session.commit()
                return True
            except Exception as e:
                print(e)
                await session.rollback()
                return False

async def sql_get_emby(tg):
    """
    查询一条emby记录，可以根据tg, embyid或者name来查询
    """
    async with Session() as session:
        try:
            if isinstance(tg, int) or (isinstance(tg, str) and tg.isdigit()):
                condition = or_(
                    Emby.tg == int(tg),
                    Emby.name == str(tg),
                    Emby.embyid == str(tg)
                )
            else:
                condition = or_(
                    Emby.name == str(tg),
                    Emby.embyid == str(tg)
                )
            result = await session.execute(select(Emby).filter(condition))
            emby = result.scalars().first()
            if emby:
                # 显式地加载并 expunge 对像，使其可以在 session.commit() 之后访问
                session.expunge(emby)
            return emby
        except:
            return None

async def get_all_emby(condition):
    """
    查询所有emby记录
    """
    async with Session() as session:
        try:
            result = await session.execute(select(Emby).filter(condition))
            embies = result.scalars().all()
            for emby in embies:
                session.expunge(emby)
            return embies
        except:
            return None

async def sql_update_emby(condition, **kwargs):
    """
    更新一条emby记录，根据condition来匹配，然后更新其他的字段
    """
    async with Session() as session:
        try:
            result = await session.execute(select(Emby).filter(condition))
            emby = result.scalars().first()
            if emby is None:
                return False
            for k, v in kwargs.items():
                setattr(emby, k, v)
            await session.commit()
            return True
        except Exception as e:
            LOGGER.error(e)
            return False

async def sql_count_emby():
    """
    # 检索有tg和embyid的emby记录的数量，以及Emby.lv =='a'条件下的数量
    :return: int, int, int
    """
    async with Session() as session:
        try:
            result = await session.execute(
                select(
                    func.count(Emby.tg).label("tg_count"),
                    func.count(Emby.embyid).label("embyid_count"),
                    func.count(case((Emby.lv == "a", 1))).label("lv_a_count")
                )
            )
            count = result.first()
        except Exception as e:
            return None, None, None
        else:
            return count.tg_count, count.embyid_count, count.lv_a_count
