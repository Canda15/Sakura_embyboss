from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from sqlalchemy import BigInteger, Column, DateTime, Integer, String, select, update, delete

from bot.sql_helper import Base, Session, engine


class PartitionCode(Base):
    __tablename__ = "partition_codes"

    code = Column(String(50), primary_key=True, autoincrement=False)
    partition = Column(String(64), nullable=False)
    duration_days = Column(Integer, nullable=False, default=1)
    created_by = Column(BigInteger, nullable=True)
    created_at = Column(DateTime, default=datetime.now)


# PartitionCode.__table__.create(bind=engine, checkfirst=True)


class PartitionGrant(Base):
    __tablename__ = "partition_grants"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tg = Column(BigInteger, nullable=False, index=True)
    embyid = Column(String(255), nullable=True)
    partition = Column(String(64), nullable=False)
    expires_at = Column(DateTime, nullable=False)
    status = Column(String(20), default="active", index=True)
    code = Column(String(50), nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


# PartitionGrant.__table__.create(bind=engine, checkfirst=True)


async def sql_add_partition_codes(items: List[Dict]) -> bool:
    """批量插入分区码记录。items 需包含 code/partition/duration_days/created_by/expires_at(optional)。"""
    async with Session() as session:
        try:
            rows = [PartitionCode(**item) for item in items]
            session.add_all(rows)
            await session.commit()
            return True
        except Exception:
            await session.rollback()
            return False


async def sql_get_partition_code(code: str) -> Optional[PartitionCode]:
    async with Session() as session:
        result = await session.execute(select(PartitionCode).filter(PartitionCode.code == code))
        record = result.scalars().first()
        if record:
            session.expunge(record)
        return record


async def sql_delete_partition_code(code: str) -> bool:
    """使用后删除分区码，防止重复使用。"""
    async with Session() as session:
        result = await session.execute(select(PartitionCode).filter(PartitionCode.code == code))
        row = result.scalars().first()
        if not row:
            return False
        try:
            await session.delete(row)
            await session.commit()
            return True
        except Exception:
            await session.rollback()
            return False


async def sql_upsert_partition_grant(tg: int, embyid: str, partition: str, expires_at: datetime, code: str = None) -> bool:
    """插入或延长分区授权。若已有该用户同分区记录则延长到新的 expires_at (取较大者)。"""
    async with Session() as session:
        try:
            result = await session.execute(
                select(PartitionGrant)
                .filter(PartitionGrant.tg == tg, PartitionGrant.partition == partition)
                .with_for_update()
            )
            grant = result.scalars().first()
            if grant:
                if expires_at > grant.expires_at:
                    grant.expires_at = expires_at
                grant.status = "active"
                grant.code = code or grant.code
                grant.updated_at = datetime.now()
            else:
                grant = PartitionGrant(
                    tg=tg,
                    embyid=embyid,
                    partition=partition,
                    expires_at=expires_at,
                    status="active",
                    code=code,
                )
                session.add(grant)
            await session.commit()
            return True
        except Exception:
            await session.rollback()
            return False


async def sql_get_active_grants_by_user(tg: int, now: datetime) -> List[PartitionGrant]:
    async with Session() as session:
        result = await session.execute(
            select(PartitionGrant)
            .filter(
                PartitionGrant.tg == tg,
                PartitionGrant.status == "active",
                PartitionGrant.expires_at > now,
            )
        )
        grants = result.scalars().all()
        for g in grants:
            session.expunge(g)
        return grants


async def sql_get_active_grants_for_users(user_ids: List[int], now: datetime) -> Dict[int, List[PartitionGrant]]:
    if not user_ids:
        return {}
    async with Session() as session:
        result = await session.execute(
            select(PartitionGrant)
            .filter(
                PartitionGrant.tg.in_(user_ids),
                PartitionGrant.status == "active",
                PartitionGrant.expires_at > now,
            )
        )
        rows = result.scalars().all()
        for r in rows:
            session.expunge(r)
    result_dict: Dict[int, List[PartitionGrant]] = {}
    for row in rows:
        result_dict.setdefault(row.tg, []).append(row)
    return result_dict


async def sql_get_expired_grants(now: datetime) -> List[PartitionGrant]:
    async with Session() as session:
        result = await session.execute(
            select(PartitionGrant)
            .filter(
                PartitionGrant.status == "active",
                PartitionGrant.expires_at <= now,
            )
        )
        grants = result.scalars().all()
        for g in grants:
            session.expunge(g)
        return grants


async def sql_mark_grants_expired(ids: List[int]) -> None:
    if not ids:
        return
    async with Session() as session:
        try:
            await session.execute(
                update(PartitionGrant)
                .where(PartitionGrant.id.in_(ids))
                .values(status="expired", updated_at=datetime.now())
            )
            await session.commit()
        except Exception:
            await session.rollback()


async def sql_list_partition_codes(limit: int = 50, offset: int = 0) -> List[PartitionCode]:
    async with Session() as session:
        result = await session.execute(
            select(PartitionCode)
            .order_by(PartitionCode.created_at.desc())
            .offset(offset)
            .limit(limit)
        )
        codes = result.scalars().all()
        for c in codes:
            session.expunge(c)
        return codes


async def sql_list_partition_grants(limit: int = 50, offset: int = 0) -> List[PartitionGrant]:
    async with Session() as session:
        result = await session.execute(
            select(PartitionGrant)
            .order_by(PartitionGrant.created_at.desc())
            .offset(offset)
            .limit(limit)
        )
        grants = result.scalars().all()
        for g in grants:
            session.expunge(g)
        return grants


async def sql_count_partition_codes() -> int:
    async with Session() as session:
        from sqlalchemy import func
        result = await session.execute(select(func.count(PartitionCode.code)))
        return result.scalar()


async def sql_count_partition_grants() -> int:
    async with Session() as session:
        from sqlalchemy import func
        result = await session.execute(select(func.count(PartitionGrant.id)))
        return result.scalar()


async def sql_delete_partition_code_or_grant_by_code(code: str) -> Tuple[int, int]:
    async with Session() as session:
        try:
            now = datetime.now()
            res1 = await session.execute(
                delete(PartitionCode).where(PartitionCode.code == code)
            )
            unused_deleted = res1.rowcount
            res2 = await session.execute(
                delete(PartitionGrant)
                .where(PartitionGrant.code == code)
                .where((PartitionGrant.status != "active") | (PartitionGrant.expires_at <= now))
            )
            used_deleted = res2.rowcount
            await session.commit()
            return unused_deleted, used_deleted
        except Exception:
            await session.rollback()
            return 0, 0


async def sql_clear_unused_partition_codes() -> int:
    async with Session() as session:
        try:
            result = await session.execute(delete(PartitionCode))
            count = result.rowcount
            await session.commit()
            return count
        except Exception:
            await session.rollback()
            return 0


async def sql_clear_used_partition_grants() -> int:
    async with Session() as session:
        try:
            now = datetime.now()
            result = await session.execute(
                delete(PartitionGrant)
                .where((PartitionGrant.status != "active") | (PartitionGrant.expires_at <= now))
            )
            count = result.rowcount
            await session.commit()
            return count
        except Exception:
            await session.rollback()
            return 0


async def sql_clear_all_partition_data() -> int:
    async with Session() as session:
        try:
            result = await session.execute(delete(PartitionCode))
            count = result.rowcount
            await session.commit()
            return count
        except Exception:
            await session.rollback()
            return 0


async def sql_redeem_partition_code_atomic(code: str, tg: int, embyid: str, now: datetime) -> Tuple[bool, Optional[str], Optional[datetime]]:
    """
    原子化兑换分区码：同一事务内完成 校验码->写入/延长授权->删除分区码。
    返回 (ok, partition, expires_at)。
    """
    async with Session() as session:
        try:
            res1 = await session.execute(
                select(PartitionCode)
                .filter(PartitionCode.code == code)
                .with_for_update()
            )
            record = res1.scalars().first()
            if not record:
                return False, None, None

            partition = record.partition
            res2 = await session.execute(
                select(PartitionGrant)
                .filter(PartitionGrant.tg == tg, PartitionGrant.partition == partition)
                .with_for_update()
            )
            grant = res2.scalars().first()

            start_from = grant.expires_at if grant and grant.expires_at > now else now
            expires_at = start_from + timedelta(days=record.duration_days)

            if grant:
                if expires_at > grant.expires_at:
                    grant.expires_at = expires_at
                grant.status = "active"
                grant.code = code
                grant.updated_at = datetime.now()
            else:
                grant = PartitionGrant(
                    tg=tg,
                    embyid=embyid,
                    partition=partition,
                    expires_at=expires_at,
                    status="active",
                    code=code,
                )
                session.add(grant)

            await session.delete(record)
            await session.commit()
            return True, partition, expires_at
        except Exception:
            await session.rollback()
            return False, None, None
