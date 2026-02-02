"""
指标计算器 - 技术指标计算
"""
import pandas as pd
from typing import Optional


def calc_ma(df: pd.DataFrame, period: int, column: str = '收盘') -> pd.Series:
    """
    计算移动平均线
    
    Args:
        df: K线数据 DataFrame
        period: 均线周期
        column: 计算列名，默认 '收盘'
    
    Returns:
        均线 Series
    """
    return df[column].rolling(window=period).mean()


def calc_volume_ma(df: pd.DataFrame, period: int, column: str = '成交量') -> pd.Series:
    """
    计算成交量均线
    
    Args:
        df: K线数据 DataFrame
        period: 均线周期
        column: 成交量列名
    
    Returns:
        成交量均线 Series
    """
    return df[column].rolling(window=period).mean()


def calc_period_change(df: pd.DataFrame, days: int, column: str = '收盘') -> float:
    """
    计算区间涨跌幅
    
    Args:
        df: K线数据 DataFrame，需按日期升序排列
        days: 回溯天数
        column: 价格列名
    
    Returns:
        涨跌幅百分比
    """
    if len(df) < days:
        return 0.0
    
    current_price = df[column].iloc[-1]
    past_price = df[column].iloc[-days] if days <= len(df) else df[column].iloc[0]
    
    if past_price == 0:
        return 0.0
    
    return (current_price - past_price) / past_price * 100


def calc_latest_vs_ma(df: pd.DataFrame, ma_period: int, column: str = '收盘') -> dict:
    """
    计算最新价与均线的关系
    
    Args:
        df: K线数据 DataFrame
        ma_period: 均线周期
        column: 价格列名
    
    Returns:
        dict: {'latest': 最新价, 'ma': 均线值, 'above': 是否在均线上方}
    """
    ma = calc_ma(df, ma_period, column)
    latest = df[column].iloc[-1]
    ma_value = ma.iloc[-1]
    
    return {
        'latest': latest,
        'ma': ma_value,
        'above': latest > ma_value
    }
