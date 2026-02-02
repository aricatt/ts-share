"""
原子过滤器 - 最小粒度的筛选条件
每个函数只做一件事，可组合使用
"""
import pandas as pd
from typing import List, Optional


def filter_by_exchange(df: pd.DataFrame, exclude: Optional[List[str]] = None) -> pd.DataFrame:
    """
    按交易所/板块过滤
    
    Args:
        df: 股票数据 DataFrame，需包含 '代码' 列
        exclude: 要排除的板块列表，可选值: ['创业板', '科创板', '北交所']
    
    Returns:
        过滤后的 DataFrame
    """
    if exclude is None:
        return df
    
    result = df.copy()
    
    if '创业板' in exclude:
        result = result[~result['代码'].str.startswith('300')]
    if '科创板' in exclude:
        result = result[~result['代码'].str.startswith('688')]
    if '北交所' in exclude:
        result = result[~result['代码'].str.startswith('8')]
    
    return result


def filter_by_st(df: pd.DataFrame, exclude: bool = True) -> pd.DataFrame:
    """
    过滤 ST 股票
    
    Args:
        df: 股票数据 DataFrame，需包含 '名称' 列
        exclude: True 排除 ST，False 保留
    
    Returns:
        过滤后的 DataFrame
    """
    if not exclude:
        return df
    
    return df[~df['名称'].str.contains('ST', case=False, na=False)]


def filter_by_turnover(
    df: pd.DataFrame, 
    min_val: Optional[float] = None, 
    max_val: Optional[float] = None
) -> pd.DataFrame:
    """
    按换手率过滤
    
    Args:
        df: 股票数据 DataFrame，需包含 '换手率' 列
        min_val: 最小换手率（%）
        max_val: 最大换手率（%）
    
    Returns:
        过滤后的 DataFrame
    """
    result = df.copy()
    
    if min_val is not None:
        result = result[result['换手率'] >= min_val]
    if max_val is not None:
        result = result[result['换手率'] <= max_val]
    
    return result


def filter_by_market_cap(
    df: pd.DataFrame,
    min_val: Optional[float] = None,
    max_val: Optional[float] = None,
    column: str = '总市值'
) -> pd.DataFrame:
    """
    按市值过滤
    
    Args:
        df: 股票数据 DataFrame
        min_val: 最小市值（元）
        max_val: 最大市值（元）
        column: 市值列名，默认 '总市值'
    
    Returns:
        过滤后的 DataFrame
    """
    result = df.copy()
    
    if min_val is not None:
        result = result[result[column] >= min_val]
    if max_val is not None:
        result = result[result[column] <= max_val]
    
    return result


def filter_by_limit_up(df: pd.DataFrame, threshold: float = 9.9) -> pd.DataFrame:
    """
    筛选涨停股
    
    Args:
        df: 股票数据 DataFrame，需包含 '涨跌幅' 列
        threshold: 涨停阈值，默认 9.9%
    
    Returns:
        涨停股 DataFrame
    """
    return df[df['涨跌幅'] >= threshold]


def filter_by_change(
    df: pd.DataFrame,
    min_val: Optional[float] = None,
    max_val: Optional[float] = None,
    column: str = '涨跌幅'
) -> pd.DataFrame:
    """
    按涨跌幅过滤
    
    Args:
        df: 股票数据 DataFrame
        min_val: 最小涨跌幅（%）
        max_val: 最大涨跌幅（%）
        column: 涨跌幅列名
    
    Returns:
        过滤后的 DataFrame
    """
    result = df.copy()
    
    if min_val is not None:
        result = result[result[column] >= min_val]
    if max_val is not None:
        result = result[result[column] <= max_val]
    
    return result
