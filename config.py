"""
TS-Share 配置文件
"""

# 缓存配置
CACHE_DIR = ".cache"
CACHE_EXPIRE_DAYS = 1  # 当日数据缓存过期时间

# 默认排除的板块
DEFAULT_EXCLUDE_EXCHANGES = ["创业板", "科创板", "北交所"]

# 默认排除 ST
DEFAULT_EXCLUDE_ST = True

# 市值单位（亿）
MARKET_CAP_UNIT = 1e8
