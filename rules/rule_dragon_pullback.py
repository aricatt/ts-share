"""
策略：龙回头 (Dragon Pullback)

筛选条件：
- 近4个月涨幅 > 50%
- 近1个月跌幅 > 10%
- 收盘价 > 60日均线
- 收盘价 < 20日均线
- 成交量 < 5日均量线
- 换手率 < 13%
- 非ST
- 非科创板
- 非北交所
"""
import pandas as pd
from .base import BaseRule
from filters import filter_by_exchange, filter_by_st, filter_by_turnover
from indicators import calc_ma, calc_volume_ma, calc_period_change
from config import MARKET_CAP_UNIT


class RuleDragonPullback(BaseRule):
    """龙回头策略"""
    
    name = "龙回头"
    description = "曾涨停后回调，4月涨50%后回调，站上MA60但在MA20下"
    # 使用历史涨停股池作为初始筛选池
    # 龙回头是捕捉曾经强势但现在回调稳定的股票
    data_source = "historical_zt"
    requires_history = True
    history_days = 120  # 需要约4个月数据
    
    def __init__(
        self,
        min_4m_change: float = 50.0,    # 4月最小涨幅
        max_1m_change: float = -10.0,   # 1月最大跌幅（负数）
        max_turnover: float = 13.0,
        exclude_exchanges: list = None,
        exclude_st: bool = True,
    ):
        super().__init__()  # 初始化 tracker
        self.min_4m_change = min_4m_change
        self.max_1m_change = max_1m_change
        self.max_turnover = max_turnover
        self.exclude_exchanges = exclude_exchanges or ["科创板", "北交所"]
        self.exclude_st = exclude_st
    
    def apply(self, df: pd.DataFrame, history_provider=None, **kwargs) -> pd.DataFrame:
        """
        应用策略筛选
        
        Args:
            df: 当日股票数据
            history_provider: 历史数据获取器，需提供 get_history(code, days) 方法
        """
        result = df.copy()
        
        # 开始跟踪
        self.tracker.start(result)
        
        # 1. 排除板块（先做基础筛选，减少历史数据查询量）
        result = filter_by_exchange(result, exclude=self.exclude_exchanges)
        self.tracker.record(
            "排除板块",
            result,
            f"排除: {', '.join(self.exclude_exchanges)}"
        )
        
        # 2. 排除 ST
        result = filter_by_st(result, exclude=self.exclude_st)
        self.tracker.record(
            "排除ST",
            result,
            f"排除ST: {self.exclude_st}"
        )
        
        # 3. 换手率筛选
        result = filter_by_turnover(result, max_val=self.max_turnover)
        self.tracker.record(
            "换手率筛选",
            result,
            f"换手率 <= {self.max_turnover}%"
        )
        
        if history_provider is None:
            self.tracker.record(
                "历史数据检查",
                result,
                "⚠️ 未提供历史数据，跳过历史条件检查"
            )
            return result
        
        # 4. 逐只股票检查历史条件
        before_history = len(result)
        valid_codes = []
        for _, row in result.iterrows():
            code = row['代码']
            try:
                hist = history_provider.get_history(code, self.history_days)
                if hist is not None and self._check_history_conditions(hist):
                    valid_codes.append(code)
            except Exception as e:
                print(f"获取 {code} 历史数据失败: {e}")
                continue
        
        result = result[result['代码'].isin(valid_codes)]
        self.tracker.record(
            "历史条件筛选",
            result,
            f"4月涨>{self.min_4m_change}%, 1月跌>{abs(self.max_1m_change)}%, MA条件"
        )
        
        return result
    
    def _check_history_conditions(self, hist: pd.DataFrame) -> bool:
        """检查历史数据条件"""
        if len(hist) < 60:  # 至少需要60天数据
            return False
        
        # 1. 近4月涨幅 > 50%
        change_4m = calc_period_change(hist, 80)
        if change_4m < self.min_4m_change:
            return False
        
        # 2. 近1月跌幅 > 10%（即涨幅 < -10%）
        change_1m = calc_period_change(hist, 20)
        if change_1m > self.max_1m_change:
            return False
        
        # 3. 收盘价 > MA60
        ma60 = calc_ma(hist, 60).iloc[-1]
        latest_close = hist['收盘'].iloc[-1]
        if latest_close <= ma60:
            return False
        
        # 4. 收盘价 < MA20
        ma20 = calc_ma(hist, 20).iloc[-1]
        if latest_close >= ma20:
            return False
        
        # 5. 成交量 < 5日均量
        vol_ma5 = calc_volume_ma(hist, 5).iloc[-1]
        latest_vol = hist['成交量'].iloc[-1]
        if latest_vol >= vol_ma5:
            return False
        
        return True
    
    def get_params(self):
        """获取策略参数"""
        return {
            "min_4m_change": {"label": "4月最小涨幅(%)", "value": self.min_4m_change, "type": "float"},
            "max_1m_change": {"label": "1月最大跌幅(%)", "value": self.max_1m_change, "type": "float"},
            "max_turnover": {"label": "最大换手率(%)", "value": self.max_turnover, "type": "float"},
            "exclude_exchanges": {"label": "排除板块", "value": self.exclude_exchanges, "type": "list"},
            "exclude_st": {"label": "排除ST", "value": self.exclude_st, "type": "bool"},
        }
