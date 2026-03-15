from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, UniqueConstraint, select, delete, update
from bot.sql_helper import Base, engine, Session
from bot import LOGGER

class EmbyFavorites(Base):
    """Emby收藏记录表"""
    __tablename__ = 'emby_favorites'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    embyid = Column(String(64), nullable=False, comment="Emby用户ID")
    embyname = Column(String(128), nullable=False, comment="Emby用户名")
    item_id = Column(String(64), nullable=False, comment="Emby项目ID")
    item_name = Column(String(256), nullable=False, comment="项目名称")
    created_at = Column(DateTime, default=datetime.now, comment="收藏时间")
    
    # 创建联合唯一索引
    __table_args__ = (
        UniqueConstraint('embyid', 'item_id', name='uix_emby_item'),
    ) 

# EmbyFavorites.__table__.create(bind=engine, checkfirst=True)

async def sql_add_favorites(embyid: str, embyname: str, item_id: str, item_name: str, is_favorite: bool = True) -> bool:
    """
    添加或删除收藏记录
    以 emby_name 为主要判断依据，因为 emby_id 可能会变化
    """
    try:
        async with Session() as session:
            if is_favorite:
                # 收藏操作：以 embyname 为主要标识符
                result = await session.execute(
                    select(EmbyFavorites).filter(
                        EmbyFavorites.embyname == embyname,
                        EmbyFavorites.item_id == item_id
                    )
                )
                existing_list = result.scalars().all()
                
                if existing_list:
                    if len(existing_list) > 1:
                        LOGGER.warning(f"发现 {len(existing_list)} 个重复收藏记录: {embyname} -> {item_name}，清理重复记录")
                        keep_record = existing_list[0]
                        for duplicate in existing_list[1:]:
                            await session.delete(duplicate)
                            LOGGER.info(f"删除重复收藏记录: {embyname} -> {item_name} (ID: {duplicate.id})")
                    else:
                        keep_record = existing_list[0]
                    
                    old_embyid = keep_record.embyid
                    keep_record.embyid = embyid
                    keep_record.item_name = item_name
                    keep_record.created_at = datetime.now()
                    
                    if old_embyid != embyid:
                        LOGGER.info(f"更新收藏记录: {embyname} -> {item_name} (EmbyID: {old_embyid} -> {embyid})")
                    else:
                        LOGGER.info(f"刷新收藏记录: {embyname} -> {item_name}")
                else:
                    favorite = EmbyFavorites(
                        embyid=embyid,
                        embyname=embyname,
                        item_id=item_id,
                        item_name=item_name
                    )
                    session.add(favorite)
                    LOGGER.info(f"新增收藏记录: {embyname} -> {item_name} (EmbyID: {embyid})")
                    
            else:
                # 取消收藏操作
                result = await session.execute(
                    select(EmbyFavorites).filter(
                        EmbyFavorites.embyname == embyname,
                        EmbyFavorites.item_id == item_id
                    )
                )
                records_to_delete = result.scalars().all()
                
                if records_to_delete:
                    if len(records_to_delete) > 1:
                        LOGGER.warning(f"发现 {len(records_to_delete)} 个重复收藏记录，全部删除: {embyname} -> {item_name}")
                    
                    for record in records_to_delete:
                        await session.delete(record)
                    
                    LOGGER.info(f"删除收藏记录: {embyname} -> {item_name} (删除了 {len(records_to_delete)} 条记录)")
                else:
                    LOGGER.info(f"未找到要删除的收藏记录: {embyname} -> {item_name}")
                    
            await session.commit()
            return True
            
    except Exception as e:
        LOGGER.error(f"操作收藏记录失败: {str(e)}")
        return False
    
async def sql_clear_favorites(emby_name: str) -> bool:
    """清除Emby用户的收藏记录"""
    try:
        async with Session() as session:
            await session.execute(delete(EmbyFavorites).filter(EmbyFavorites.embyname == emby_name))
            await session.commit()
        return True
    except Exception as e:
        LOGGER.error(f"清除收藏记录失败: {str(e)}")
        return False

async def sql_get_favorites(embyid: str, page: int = 1, page_size: int = 20) -> list:
    """获取Emby用户的收藏记录"""
    try:
        async with Session() as session:
            result = await session.execute(
                select(EmbyFavorites)
                .filter(EmbyFavorites.embyid == embyid)
                .offset((page - 1) * page_size)
                .limit(page_size)
            )
            records = result.scalars().all()
            for r in records:
                session.expunge(r)
            return records
    except Exception as e:
        LOGGER.error(f"获取收藏记录失败: {str(e)}")
        return []
    
async def sql_update_favorites(condition, **kwargs):
    """
    更新收藏记录，处理唯一约束冲突
    """
    async with Session() as session:
        try:
            result = await session.execute(select(EmbyFavorites).filter(condition))
            favorites = result.scalars().all()
            if not favorites:
                return True 

            new_embyid = kwargs.get('embyid')
            if not new_embyid:
                for favorite in favorites:
                    for k, v in kwargs.items():
                        setattr(favorite, k, v)
                await session.commit()
                LOGGER.info(f"收藏记录更新完成，成功更新 {len(favorites)} 条记录")
                return True
            
            success_count = 0
            items_to_update = {} 
            
            for favorite in favorites:
                try:
                    item_id = favorite.item_id
                    combination_key = (new_embyid, item_id)
                    
                    if combination_key in items_to_update:
                        LOGGER.warning(f"删除重复收藏记录(批次内重复): {favorite.embyname} -> {favorite.item_name}")
                        await session.delete(favorite)
                        continue
                    
                    res_existing = await session.execute(
                        select(EmbyFavorites).filter(
                            EmbyFavorites.embyid == new_embyid,
                            EmbyFavorites.item_id == item_id,
                            EmbyFavorites.id != favorite.id
                        )
                    )
                    existing = res_existing.scalars().first()
                    
                    if existing:
                        LOGGER.warning(f"删除重复收藏记录(数据库冲突): {favorite.embyname} -> {favorite.item_name}")
                        await session.delete(favorite)
                    else:
                        items_to_update[combination_key] = favorite
                        for k, v in kwargs.items():
                            setattr(favorite, k, v)
                        success_count += 1
                        
                except Exception as e:
                    LOGGER.error(f"处理单条收藏记录失败: {str(e)}")
                    continue
                    
            await session.commit()
            LOGGER.info(f"收藏记录更新完成，成功更新 {success_count} 条记录，删除 {len(favorites) - success_count} 条重复记录")
            return True
            
        except Exception as e:
            await session.rollback()
            LOGGER.error(f"更新收藏记录失败: {str(e)}")
            return False