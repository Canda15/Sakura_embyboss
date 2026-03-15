#! /usr/bin/python3
# -*- coding: utf-8 -*-
import asyncio
import pyrogram.utils
pyrogram.utils.MIN_CHANNEL_ID = -10099999999999

from bot import bot

# 面板
from bot.modules.panel import *
# 命令
from bot.modules.commands import *
# 其他
from bot.modules.extra import *
from bot.modules.callback import *
from bot.web import *
from bot.sql_helper import init_db

async def startup():
    await init_db()

asyncio.get_event_loop().run_until_complete(startup())
bot.run()
