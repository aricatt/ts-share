"""
通用 UI 组件
"""
import streamlit as st
from datetime import datetime, timedelta
from typing import Optional, List
from rules import get_all_rules, get_rule


def date_selector(key: str = "date", default_offset: int = 1) -> str:
    """
    日期选择器
    
    Args:
        key: Streamlit 组件 key
        default_offset: 默认偏移天数（1=昨天）
    
    Returns:
        日期字符串 YYYYMMDD
    """
    default_date = datetime.now() - timedelta(days=default_offset)
    selected = st.date_input("选择日期", value=default_date, key=key)
    return selected.strftime("%Y%m%d")


def rule_selector(key: str = "rule") -> Optional[str]:
    """
    策略选择器
    
    Args:
        key: Streamlit 组件 key
    
    Returns:
        选中的策略名称
    """
    rules = get_all_rules()
    selected = st.selectbox("选择策略", options=rules, key=key)
    
    # 显示策略描述
    if selected:
        rule = get_rule(selected)
        st.caption(f"📝 {rule.description}")
    
    return selected


def filter_panel(key_prefix: str = "filter") -> dict:
    """
    通用筛选面板
    
    Args:
        key_prefix: 组件 key 前缀
    
    Returns:
        筛选条件字典
    """
    st.subheader("🔍 筛选条件")
    
    col1, col2 = st.columns(2)
    
    with col1:
        max_turnover = st.slider(
            "最大换手率 (%)", 
            min_value=1, 
            max_value=50, 
            value=10,
            key=f"{key_prefix}_turnover"
        )
        
        max_market_cap = st.slider(
            "最大市值 (亿)", 
            min_value=10, 
            max_value=500, 
            value=100,
            key=f"{key_prefix}_market_cap"
        )
    
    with col2:
        exclude_gem = st.checkbox("排除创业板 (300)", value=True, key=f"{key_prefix}_gem")
        exclude_star = st.checkbox("排除科创板 (688)", value=True, key=f"{key_prefix}_star")
        exclude_bse = st.checkbox("排除北交所 (8)", value=True, key=f"{key_prefix}_bse")
        exclude_st = st.checkbox("排除ST股", value=True, key=f"{key_prefix}_st")
    
    # 构建排除板块列表
    exclude_exchanges = []
    if exclude_gem:
        exclude_exchanges.append("创业板")
    if exclude_star:
        exclude_exchanges.append("科创板")
    if exclude_bse:
        exclude_exchanges.append("北交所")
    
    return {
        "max_turnover": max_turnover,
        "max_market_cap": max_market_cap,
        "exclude_exchanges": exclude_exchanges,
        "exclude_st": exclude_st,
    }


def result_stats(df, total_before: int):
    """
    显示结果统计
    
    Args:
        df: 筛选后的 DataFrame
        total_before: 筛选前总数
    """
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📊 原始数量", total_before)
    with col2:
        st.metric("✅ 筛选后", len(df))
    with col3:
        ratio = len(df) / total_before * 100 if total_before > 0 else 0
        st.metric("📈 筛选比例", f"{ratio:.1f}%")
    with col4:
        avg_cap = df['总市值'].mean() / 1e8 if len(df) > 0 else 0
        st.metric("💰 平均市值", f"{avg_cap:.1f}亿")
