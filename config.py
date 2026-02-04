"""
TS-Share 配置文件
"""
import os

# ==================== Tushare Pro 配置 ====================
# Token 获取方式：https://tushare.pro/register 注册后在个人中心获取
# 配置方式（优先级从高到低）：
#   1. 环境变量: export TUSHARE_TOKEN=your_token_here
#   2. 在此处直接填写（不推荐提交到代码仓库）
TUSHARE_TOKEN = os.environ.get("TUSHARE_TOKEN", "2d6c62b0380588c003c2581d7b5cd418c5b31ca4fb54f232d61d3555")

# 数据源选择: "tushare" 或 "akshare"
# tushare: 需要 token，数据更稳定，支持实时行情
# akshare: 免费，无需 token，但可能被限流
DATA_SOURCE = os.environ.get("DATA_SOURCE", "tushare" if TUSHARE_TOKEN else "akshare")

# ==================== 缓存配置 ====================
CACHE_DIR = ".cache"
CACHE_EXPIRE_DAYS = 1  # 当日数据缓存过期时间

# ==================== 股票筛选默认配置 ====================
# 默认排除的板块
DEFAULT_EXCLUDE_EXCHANGES = ["创业板", "科创板", "北交所"]

# 默认排除 ST
DEFAULT_EXCLUDE_ST = True

# 市值换算单位：从“亿”换算为数据库单位“万元”
MARKET_CAP_UNIT = 10000.0

# ==================== Tushare Pro API 接口映射 ====================
# 以下是 tushare pro 常用接口列表（需要相应积分权限）
TUSHARE_API_REFERENCE = {
    # 基础数据
    "stock_basic": "股票列表（基础信息）",
    "trade_cal": "交易日历",
    "namechange": "股票曾用名",
    "stk_managers": "上市公司管理层",
    
    # 行情数据
    "daily": "日线行情",
    "weekly": "周线行情", 
    "monthly": "月线行情",
    "adj_factor": "复权因子",
    "daily_basic": "每日指标（PE/PB/换手率等）",
    "stk_limit": "每日涨跌停价格",
    "suspend_d": "每日停复牌信息",
    
    # 实时行情（重要！）
    "realtime_quote": "实时行情（盘中）",
    "realtime_list": "实时行情列表",
    "realtime_tick": "实时Tick",
    
    # 财务数据
    "income": "利润表",
    "balancesheet": "资产负债表",
    "cashflow": "现金流量表",
    "fina_indicator": "财务指标",
    "forecast": "业绩预告",
    "express": "业绩快报",
    
    # 打板专题数据（选股器核心）
    "limit_list_d": "涨跌停统计（当日）",
    "ths_hot": "同花顺热榜",
    "dc_hot": "东方财富热榜",
    "stk_surv": "机构调研数据",
    "top_list": "龙虎榜每日统计",
    "top_inst": "龙虎榜机构交易",
    
    # 资金流向
    "moneyflow": "个股资金流向",
    "moneyflow_hsgt": "沪深港通资金流向",
    
    # 概念板块
    "ths_index": "同花顺指数列表",
    "ths_daily": "同花顺指数行情",
    "ths_member": "同花顺成分股",
}
