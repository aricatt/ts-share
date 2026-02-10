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
        # 兼容 300, 301 等所有创业板号段
        result = result[~result['代码'].str.startswith('30')]
    if '科创板' in exclude:
        result = result[~result['代码'].str.startswith('688')]
    if '北交所' in exclude:
        # 北交所主要包含 43, 83, 87, 88, 82 等号段
        result = result[~result['代码'].str.startswith(('43', '83', '87', '88', '82', '9'))]
    
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


def filter_by_limit_up(df: pd.DataFrame) -> pd.DataFrame:
    """
    筛选涨停股 (自动识别板块差异化涨幅)
    
    判定标准：
    - 创业板/科创板 (300/688): >= 19.9%
    - 北交所 (8/4/9): >= 29.9%
    - ST 股票: >= 4.9%
    - 主板/其他: >= 9.9%
    
    Args:
        df: 股票数据 DataFrame，需包含 '代码' 列，可选包含 '名称' 列
    
    Returns:
        涨停股 DataFrame
    """
    if df.empty:
        return df
        
    # 判定标准：为了兼容低价股精度导致的 9.9x% 涨停，放宽到 9.8%
    # 默认阈值为 10cm (主板)
    thresholds = pd.Series(9.8, index=df.index)
    
    if '代码' in df.columns:
        # 创业板(30) 和 科创板(688) 为 20cm
        thresholds.loc[df['代码'].str.startswith(('30', '688'))] = 19.8
        # 北交所 (8/4/9) 为 30cm
        thresholds.loc[df['代码'].str.startswith(('8', '4', '9'))] = 29.8
        
    if '名称' in df.columns:
        # ST 股为 5cm
        thresholds.loc[df['名称'].str.contains('ST', case=False, na=False)] = 4.8
    
    return df[df['涨跌幅'] >= thresholds]


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

def filter_by_not_one_word(df: pd.DataFrame) -> pd.DataFrame:
    """
    过滤一字板
    
    一字板判定标准：开盘价 == 收盘价
    非一字板：开盘价 != 收盘价
    
    Args:
        df: 股票数据 DataFrame，需包含 '开盘' 和 '收盘' 或相应价格列
    
    Returns:
        过滤后的 DataFrame
    """
    # 兼容不同数据源的列名
    open_col = '开盘' if '开盘' in df.columns else ('open' if 'open' in df.columns else None)
    close_col = '收盘' if '收盘' in df.columns else ('close' if 'close' in df.columns else None)
    
    if open_col and close_col:
        return df[df[open_col] != df[close_col]]
    return df

def filter_by_industry(df: pd.DataFrame, industries: Optional[List[str]] = None) -> pd.DataFrame:
    """
    按行业名称过滤
    
    Args:
        df: 股票数据 DataFrame，需包含 '行业' 列
        industries: 需要包含的行业列表
    
    Returns:
        过滤后的 DataFrame
    """
    if not industries:
        return df
    
    return df[df['行业'].isin(industries)]
