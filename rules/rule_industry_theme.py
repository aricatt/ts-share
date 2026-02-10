"""
策略：行业板块筛选 (Industry & Theme Screening)

筛选条件：
- 指定行业
- 关键词搜索（用于题材筛选）
- 市值区间
- 换手率区间
- 涨跌幅区间
"""
import pandas as pd
from .base import BaseRule
from filters.atomic import (
    filter_by_industry,
    filter_by_market_cap,
    filter_by_turnover,
    filter_by_change,
    filter_by_st,
    filter_by_exchange
)
from config import MARKET_CAP_UNIT

class RuleIndustryTheme(BaseRule):
    """行业板块筛选策略"""
    
    name = "行业板块筛选"
    description = "按行业、题材关键词、市值、换手等维度进行全方位筛选"
    
    # 默认从全A股筛选
    data_source = "all_stocks"
    requires_history = False
    
    def __init__(
        self,
        industries: list = None,
        keywords: str = "",
        min_market_cap: float = 0.0,
        max_market_cap: float = 1000.0,
        min_turnover: float = 0.0,
        max_turnover: float = 20.0,
        min_pct_chg: float = -10.0,
        max_pct_chg: float = 10.0,
        exclude_st: bool = True,
        exclude_exchanges: list = None
    ):
        super().__init__()
        self.industries = industries or []
        self.keywords = keywords
        self.min_market_cap = min_market_cap * MARKET_CAP_UNIT
        self.max_market_cap = max_market_cap * MARKET_CAP_UNIT
        self.min_turnover = min_turnover
        self.max_turnover = max_turnover
        self.min_pct_chg = min_pct_chg
        self.max_pct_chg = max_pct_chg
        self.exclude_st = exclude_st
        self.exclude_exchanges = exclude_exchanges or []
        
    def apply(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """应用筛选逻辑"""
        result = df.copy()
        self.tracker.start(result)
        
        # 1. 行业筛选
        if self.industries:
            result = filter_by_industry(result, self.industries)
            self.tracker.record("行业筛选", result, f"属于: {', '.join(self.industries)}")
            
        # 2. 题材/关键词筛选 (模糊匹配名称、行业、主营业务、概念)
        if self.keywords:
            # 这里的字段来源于 StockService.get_daily_all() 关联后的结果
            search_cols = [c for c in ['名称', '行业', '主营业务', '概念'] if c in result.columns]
            if search_cols:
                mask = result[search_cols].apply(lambda x: x.str.contains(self.keywords, case=False, na=False)).any(axis=1)
                result = result[mask]
                self.tracker.record("题材筛选", result, f"关键词包含: {self.keywords}")
            
        # 3. 基础过滤（ST/板块）
        result = filter_by_st(result, exclude=self.exclude_st)
        self.tracker.record("排除ST", result, f"排除ST: {self.exclude_st}")
        
        if self.exclude_exchanges:
            result = filter_by_exchange(result, exclude=self.exclude_exchanges)
            self.tracker.record("板块过滤", result, f"排除: {', '.join(self.exclude_exchanges)}")
            
        # 4. 行情过滤
        result = filter_by_market_cap(result, min_val=self.min_market_cap, max_val=self.max_market_cap)
        self.tracker.record("市值筛选", result, f"市值区间: {self.min_market_cap/MARKET_CAP_UNIT:.0f}-{self.max_market_cap/MARKET_CAP_UNIT:.0f} 亿")
        
        result = filter_by_turnover(result, min_val=self.min_turnover, max_val=self.max_turnover)
        self.tracker.record("换手率筛选", result, f"换手率区间: {self.min_turnover}%-{self.max_turnover}%")
        
        result = filter_by_change(result, min_val=self.min_pct_chg, max_val=self.max_pct_chg)
        self.tracker.record("价格动能筛选", result, f"本日涨跌幅: {self.min_pct_chg}% 到 {self.max_pct_chg}%")
        
        return result

    def get_params(self):
        """获取策略参数，由 StockService 动态注入行业列表"""
        return {
            "industries": {
                "label": "选择行业",
                "value": self.industries,
                "type": "multiselect",
                "options": [] # 待 UI 注入
            },
            "keywords": {
                "label": "题材关键词",
                "value": self.keywords,
                "type": "text",
                "placeholder": "如：人工智能, 华为"
            },
            "min_market_cap": {"label": "最小市值(亿)", "value": self.min_market_cap / MARKET_CAP_UNIT, "type": "float", "min": 0, "max": 2000},
            "max_market_cap": {"label": "最大市值(亿)", "value": self.max_market_cap / MARKET_CAP_UNIT, "type": "float", "min": 0, "max": 2000},
            "min_turnover": {"label": "最小换手率(%)", "value": self.min_turnover, "type": "float", "min": 0, "max": 100},
            "max_turnover": {"label": "最大换手率(%)", "value": self.max_turnover, "type": "float", "min": 0, "max": 100},
            "min_pct_chg": {"label": "最小涨幅(%)", "value": self.min_pct_chg, "type": "float", "min": -20, "max": 20},
            "max_pct_chg": {"label": "最大涨幅(%)", "value": self.max_pct_chg, "type": "float", "min": -20, "max": 20},
            "exclude_st": {"label": "排除ST", "value": self.exclude_st, "type": "bool"},
            "exclude_exchanges": {
                "label": "排除板块", 
                "value": self.exclude_exchanges, 
                "type": "list",
                "options": ["创业板", "科创板", "北交所"]
            },
        }
