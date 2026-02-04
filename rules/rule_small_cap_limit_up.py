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
"""
import pandas as pd
from .base import BaseRule
from filters import (
    filter_by_exchange,
    filter_by_st,
    filter_by_turnover,
    filter_by_market_cap,
    filter_by_limit_up,
    filter_by_not_one_word,
)
from config import MARKET_CAP_UNIT


class RuleSmallCapLimitUp(BaseRule):
    """小盘涨停异动策略"""
    
    name = "小盘涨停异动"
    description = "涨停板 + 非一字板 + 换手率<10% + 非创/科/北/ST + 市值<100亿"
    requires_history = False
    
    def __init__(
        self,
        max_turnover: float = 10.0,
        max_market_cap: float = 100.0,  # 亿
        exclude_exchanges: list = None,
        exclude_st: bool = True,
    ):
        super().__init__()  # 初始化 tracker
        self.max_turnover = max_turnover
        self.max_market_cap = max_market_cap * MARKET_CAP_UNIT
        self.exclude_exchanges = exclude_exchanges or ["创业板", "科创板", "北交所"]
        self.exclude_st = exclude_st
    
    def apply(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
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
        
        # 1.5. 非一字板筛选
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
        
        return result
    
    def get_params(self):
        """获取策略参数"""
        return {
            "max_turnover": {"label": "最大换手率(%)", "value": self.max_turnover, "type": "float"},
            "max_market_cap": {"label": "最大市值(亿)", "value": self.max_market_cap / MARKET_CAP_UNIT, "type": "float"},
            "exclude_exchanges": {"label": "排除板块", "value": self.exclude_exchanges, "type": "list"},
            "exclude_st": {"label": "排除ST", "value": self.exclude_st, "type": "bool"},
        }
