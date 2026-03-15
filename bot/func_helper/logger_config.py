import datetime
import pytz
import sys
from loguru import logger

# 转换为亚洲上海时区
shanghai_tz = pytz.timezone("Asia/Shanghai")
Now = datetime.datetime.now(shanghai_tz)
log_filename = f"log/log_{Now.strftime('%Y%m%d')}.txt"
log_format = "{time:YYYY-MM-DD HH:mm:ss.SSS ZZ} | {name} | {level} | {message}"

# ================= 新增配置 =================
# 清除掉 loguru 默认自带的控制台规则
logger.remove()

# 手动添加控制台的输出规则，将 level 设为 "DEBUG" (如果想看最最底层的海量信息也可以填 "TRACE")
logger.add(sys.stderr, format=log_format, level="DEBUG")
# ============================================


# 下面是原有的写入文件配置 (写入文件的依然只保留 INFO 及以上)
log_config = {
    "sink": log_filename,
    "format": log_format,
    "level": "INFO",
    "rotation": "00:00",
    "retention": "30 days"
}
logger.add(**log_config)

def logu(name):
    """返回一个绑定名称的日志实例"""
    return logger.bind(name=name)
