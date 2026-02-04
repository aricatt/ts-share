"""
策略：龙回头 (Dragon Pullback)

筛选条件：
- 进四个月涨幅超过50%
- 近一个月内跌幅超过10%
- 收盘价在60日均线之上，且在20日均线之下
- 收盘价距离60日均线更近（更靠近支撑位）
- 成交量小于5日均量线
- 换手率 < 13%
- 非ST，非科创板，非北交所
"""
import pandas as pd
from .base import BaseRule
from filters import filter_by_exchange, filter_by_st, filter_by_turnover
from indicators import calc_ma, calc_volume_ma, calc_period_change
from config import MARKET_CAP_UNIT


class RuleDragonPullback(BaseRule):
    """龙回头策略"""
    
    name = "龙回头"
    description = "曾涨停后回调，4月涨50%后回调，站上MA60但在MA20下，且靠近MA60"
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
        # 现在利用数据库中的预算指标进行加速
        valid_codes = []
        trade_date = kwargs.get('date_str')
        
        # 批量获取所有候选股的历史收盘价（用于计算涨幅和回落）
        for _, row in result.iterrows():
            code = row['代码']
            try:
                # 优先检查当前行的 DB 指标（如果存在）
                has_db_indicators = all(k in row for k in ['ma20', 'ma60', 'vma5', 'qfq_收盘'])
                
                if has_db_indicators:
                    # 直接用现有列做初步排除，减少 get_history 调用
                    latest_close = row['qfq_收盘']
                    # 1. 位置判断：MA60 < 收盘 < MA20
                    if latest_close <= row['ma60'] or latest_close >= row['ma20']:
                        continue
                    
                    # 2. 距离判断：距离MA60更近
                    if (latest_close - row['ma60']) >= (row['ma20'] - latest_close):
                        continue
                        
                    # 3. 缩量判断
                    if row['成交量'] >= row['vma5']:
                        continue
                
                # 4. 剩下的再查历史算涨幅
                hist = history_provider.get_history(code, self.history_days)
                if hist is not None and self._check_history_conditions(hist):
                    valid_codes.append(code)
            except Exception as e:
                print(f"检查 {code} 失败: {e}")
                continue
        
        result = result[result['代码'].isin(valid_codes)]
        self.tracker.record(
            "满足龙回头特征",
            result,
            f"4月涨>{self.min_4m_change}%, 1月跌>{abs(self.max_1m_change)}%, 缩量且更靠近MA60"
        )
        
        return result
    
    def _check_history_conditions(self, hist: pd.DataFrame) -> bool:
        """检查历史数据条件"""
        if len(hist) < 60:  # 至少需要60天数据
            return False
        
        # 使用前复权价格计算（避免除权干扰）
        price_col = 'qfq_收盘' if 'qfq_收盘' in hist.columns else '收盘'
        high_col = 'qfq_最高' if 'qfq_最高' in hist.columns else '最高'
        
        # 1. 近4月涨幅 > 50% (约 80 交易日)
        price_now = hist[price_col].iloc[-1]
        idx_4m = max(0, len(hist) - 81)
        price_4m_ago = hist[price_col].iloc[idx_4m]
        change_4m = (price_now / price_4m_ago - 1) * 100 if price_4m_ago > 0 else 0
        
        if change_4m < self.min_4m_change:
            return False
        
        # 2. 自近1月高点回落 > 10%
        idx_1m = max(0, len(hist) - 21)
        recent_hist = hist.iloc[idx_1m:]
        max_price_1m = recent_hist[high_col].max()
        drop_1m = (price_now / max_price_1m - 1) * 100 if max_price_1m > 0 else 0
        
        if drop_1m > self.max_1m_change: # max_1m_change 为 -10.0
            return False
        
        # 3. 收盘价位置：MA60 < 收盘 < MA20
        latest = hist.iloc[-1]
        ma60 = latest['ma60'] if 'ma60' in latest and pd.notnull(latest['ma60']) else calc_ma(hist, 60).iloc[-1]
        ma20 = latest['ma20'] if 'ma20' in latest and pd.notnull(latest['ma20']) else calc_ma(hist, 20).iloc[-1]
        
        if price_now <= ma60 or price_now >= ma20:
            return False
            
        # 3b. 距离更近判断：距离MA60更近
        if (price_now - ma60) >= (ma20 - price_now):
            return False
        
        # 4. 缩量检查：当前成交量 < 5日均量
        vma5 = latest['vma5'] if 'vma5' in latest and pd.notnull(latest['vma5']) else calc_volume_ma(hist, 5).iloc[-1]
        if latest['成交量'] >= vma5:
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
