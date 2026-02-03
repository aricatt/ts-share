# TS-Share 涨停板选股器

基于 Streamlit + **Tushare Pro** + SQLite + PyEcharts 构建的 A 股选股工具。

## 功能特性

- 📊 多种选股策略（小盘涨停异动、龙回头等）
- 📈 专业 K 线图表（PyEcharts）
- 💾 **SQLite 本地存储**（单文件，支持 SQL 查询）
- 🚀 **高效同步**（120天全市场数据约 2 分钟）
- 🔧 模块化架构（易于扩展新策略）

## 数据字段

同步的数据包含 **行情 + 指标**：

| 类型 | 字段 |
|------|------|
| **行情** | 日期, 代码, 开盘, 最高, 最低, 收盘, 涨跌幅, 成交量, 成交额 |
| **指标** | 换手率, 量比, PE, PE_TTM, PB, 总市值, 流通市值 |

## 快速开始

### 1. 配置 Tushare Token

在 `config.py` 中设置 Token：

```python
TUSHARE_TOKEN = "your_token_here"
```

> Token 获取：https://tushare.pro/register

### 2. 安装依赖

```bash
pip install -r requirements.txt
```

### 3. 同步数据

```bash
# 首次同步（120天，约 2 分钟）
python -c "
import sys; sys.path.insert(0, '.')
from services.data_sync_service import DataSyncService
sync = DataSyncService()
sync.sync_all_stocks(days=120)
"

# 或使用命令行
cd /path/to/ts-share
python services/data_sync_service.py --days 120
```

### 4. 运行应用

```bash
streamlit run app.py
```

## 数据查询示例

```python
from services.data_sync_service import DataSyncService

sync = DataSyncService()

# 获取单只股票历史
df = sync.get_stock_history('000001')

# 获取涨停股
df = sync.get_zt_stocks('20260130')

# 低 PE 小盘股筛选
df = sync.get_stocks_by_filter(
    trade_date='20260130',
    max_pe=20,
    max_market_cap=50,  # 亿
    limit=100
)

# 自定义 SQL 查询
df = sync.query("SELECT * FROM daily_data WHERE 涨跌幅 > 5 AND PE < 30")
```

## 项目结构

```
ts-share/
├── app.py                      # Streamlit 主入口
├── config.py                   # 配置文件（Token）
├── requirements.txt            # 依赖清单
│
├── data/
│   ├── stocks.db              # SQLite 数据库（同步后生成）
│   └── metadata.json          # 同步元数据
│
├── services/                   # 数据服务
│   ├── stock_service.py       # 股票数据服务
│   ├── data_sync_service.py   # 数据同步服务（SQLite）
│   ├── tushare_service.py     # Tushare Pro 封装
│   └── cache_service.py       # 缓存管理
│
├── pages/                      # Streamlit 页面
│   ├── 1_screener.py          # 选股器
│   ├── 2_kline.py             # K线分析
│   └── 3_settings.py          # 设置
│
└── ...
```

## 技术栈

- **Streamlit**: 应用框架
- **Tushare Pro**: 金融数据接口
- **SQLite**: 本地数据存储
- **PyEcharts**: 图表可视化
- **Pandas**: 数据处理

## 数据同步命令

```bash
# 查看同步状态
python services/data_sync_service.py --status

# 增量同步（默认120天）
python services/data_sync_service.py --days 120

# 强制全量同步
python services/data_sync_service.py --force

# 执行 SQL 查询
python services/data_sync_service.py --query "SELECT COUNT(*) FROM daily_data"
```

## 性能数据

| 操作 | 耗时 |
|------|------|
| 11 个交易日同步 | ~12 秒 |
| 120 个交易日同步 | ~2 分钟 |
| 数据库大小 (120天) | ~200 MB |

## License

MIT
