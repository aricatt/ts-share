"""
策略：小盘涨停异动 (Small Cap Limit Up)

筛选条件：
- 涨停板
- 非一字板
- 换手率 < 10%
- 非创业板
- 非科创板
- 非北交所
- 非ST
- 总市值 < 100亿
- 价格偏离均线：现价 > N日均线 M%
"""
import pandas as pd
import numpy as np
from .base import BaseRule
from filters import (
    filter_by_exchange,
    filter_by_st,
    filter_by_turnover,
    filter_by_market_cap,
    filter_by_limit_up,
    filter_by_not_one_word,
)
from indicators import calc_ma
from config import MARKET_CAP_UNIT


class RuleSmallCapLimitUp(BaseRule):
    """小盘涨停异动策略"""
    
    name = "小盘涨停异动"
    description = "涨停板 + 非一字板 + 换手率<10% + 非创/科/北/ST + 市值<100亿 + 价格由于均线偏离"
    
    # 该策略需要检查历史均线，建议开启历史数据
    # 回归使用 zt_pool，效率更高，配合 9.8% 阈值不再漏掉特定个股
    data_source = "zt_pool"
    requires_history = True
    history_days = 100  # 用于计算 MA60 等
    
    def __init__(
        self,
        max_turnover: float = 10.0,
        max_market_cap: float = 100.0,  # 亿
        ma_period: int = 20,            # 均线周期
        ma_dist_pct: float = 1.0,       # 偏离比例
        exclude_exchanges: list = None,
        exclude_st: bool = True,
        exclude_one_word: bool = False, # 默认改为 False，方便用户看到所有涨停，包括一字
    ):
        super().__init__()  # 初始化 tracker
        self.max_turnover = max_turnover
        self.max_market_cap = max_market_cap * MARKET_CAP_UNIT
        self.ma_period = int(ma_period)
        self.ma_dist_pct = ma_dist_pct
        self.exclude_exchanges = exclude_exchanges or [ "科创板", "北交所"]
        self.exclude_st = exclude_st
        self.exclude_one_word = exclude_one_word
    
    def apply(self, df: pd.DataFrame, history_provider=None, **kwargs) -> pd.DataFrame:
        """应用策略筛选，带步骤跟踪"""
        result = df.copy()
        
        # 开始跟踪
        self.tracker.start(result)
        
        # 1. 涨停筛选
        result = filter_by_limit_up(result)
        self.tracker.record(
            "涨停筛选", 
            result, 
            "自动识别板块涨停阈值 (主板10%/创科20%/北30%/ST5%)"
        )
        
        # 1.5. 非一字板筛选 (改为可选)
        if self.exclude_one_word:
            result = filter_by_not_one_word(result)
            self.tracker.record(
                "非一字板筛选",
                result,
                "排除开盘价等于收盘价的个股"
            )
        
        # 2. 换手率筛选
        result = filter_by_turnover(result, max_val=self.max_turnover)
        self.tracker.record(
            "换手率筛选", 
            result, 
            f"换手率 <= {self.max_turnover}%"
        )
        
        # 3. 排除板块
        result = filter_by_exchange(result, exclude=self.exclude_exchanges)
        self.tracker.record(
            "排除板块", 
            result, 
            f"排除: {', '.join(self.exclude_exchanges)}"
        )
        
        # 4. 排除 ST
        result = filter_by_st(result, exclude=self.exclude_st)
        self.tracker.record(
            "排除ST", 
            result, 
            f"排除ST: {self.exclude_st}"
        )
        
        # 5. 市值筛选
        result = filter_by_market_cap(result, max_val=self.max_market_cap)
        self.tracker.record(
            "市值筛选", 
            result, 
            f"总市值 <= {self.max_market_cap / MARKET_CAP_UNIT:.0f}亿"
        )
        
        # 6. 均线偏离筛选 (新增强化条件)
        if self.ma_dist_pct > 0:
            valid_codes = []
            ma_col = f'ma{self.ma_period}'
            
            for _, row in result.iterrows():
                code = row['代码']
                latest_price = row['收盘'] if '收盘' in row else row.get('最新价', 0)
                
                try:
                    # A. 优先尝试从当前行获取预计算的均线 (仅限 ma5, ma10, ma20)
                    ma_val = None
                    if ma_col in row and pd.notnull(row[ma_col]):
                        ma_val = row[ma_col]
                    
                    # B. 如果没有预计算值或周期不支持，则查询历史计算
                    if ma_val is None and history_provider is not None:
                        # 缓冲区间：如果要算 MA60，至少要 80 天数据
                        hist = history_provider.get_history(code, self.ma_period + 20)
                        if hist is not None and len(hist) >= self.ma_period:
                            ma_series = calc_ma(hist, self.ma_period)
                            ma_val = ma_series.iloc[-1]
                    
                    # C. 执行偏离检查
                    if ma_val is not None and ma_val > 0:
                        # 偏离率计算：(现价 - 均价) / 均价
                        dist = (latest_price / ma_val - 1) * 100
                        if dist >= self.ma_dist_pct:
                            valid_codes.append(code)
                except Exception as e:
                    print(f"MA检查报错 {code}: {e}")
                    continue
            
            result = result[result['代码'].isin(valid_codes)]
            self.tracker.record(
                "均线偏离筛选",
                result,
                f"现价 > {self.ma_period}日均线 {self.ma_dist_pct}% 以上"
            )
        
        return result
    
    def get_params(self):
        """获取策略参数"""
        return {
            "max_turnover": {"label": "最大换手率(%)", "value": self.max_turnover, "type": "float"},
            "max_market_cap": {
                "label": "最大市值(亿)", 
                "value": self.max_market_cap / MARKET_CAP_UNIT, 
                "type": "float",
                "min": 0.0,
                "max": 200.0,
                "step": 1.0
            },
            "ma_period": {
                "label": "均线周期", 
                "value": self.ma_period, 
                "type": "select", 
                "options": [5, 10, 20, 60]
            },
            "ma_dist_pct": {
                "label": "均线向上偏离(%)", 
                "value": self.ma_dist_pct, 
                "type": "float",
                "min": 0,
                "max": 20
            },
            "exclude_exchanges": {
                "label": "排除板块", 
                "value": self.exclude_exchanges, 
                "type": "list",
                "options": ["创业板", "科创板", "北交所"]
            },
            "exclude_st": {"label": "排除ST", "value": self.exclude_st, "type": "bool"},
            "exclude_one_word": {"label": "排除一字板", "value": self.exclude_one_word, "type": "bool"},
        }
