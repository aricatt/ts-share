"""
数据浏览器页面
展示本地 SQLite 数据库中的同步数据，支持分页和筛选
"""
import streamlit as st
import pandas as pd
import sys
import os

sys.path.insert(0, '.')
from services import DataSyncService

# 页面配置
st.set_page_config(page_title="数据浏览器 - TS-Share", page_icon="📁", layout="wide")

st.title("📁 数据浏览器")
st.markdown("浏览本地 SQLite 数据库中的全市场同步数据")

# 初始化同步服务
sync_service = DataSyncService()

# 检查数据库
if not os.path.exists(sync_service.db_path):
    st.warning("⚠️ 数据库文件不存在，请先前往「系统设置」同步数据。")
    if st.button("前往同步"):
        st.switch_page("pages/3_settings.py")
    st.stop()

# ========== 侧边栏筛选器 ==========
st.sidebar.header("🔍 数据筛选")

# 获取已同步的日期
synced_dates = sync_service.get_synced_dates()
if not synced_dates:
    st.warning("📭 数据库为空，请先同步数据。")
    st.stop()

# 默认选择最近一个交易日
latest_date = synced_dates[-1]
selected_date = st.sidebar.selectbox("选择日期", options=reversed(synced_dates), index=0)

# 其他筛选条件
st.sidebar.markdown("---")
st.sidebar.subheader("指标筛选")

min_pct_chg = st.sidebar.number_input("最小涨跌幅 (%)", value=-10.0, step=1.0)
max_pct_chg = st.sidebar.number_input("最大涨跌幅 (%)", value=20.0, step=1.0)

col1, col2 = st.sidebar.columns(2)
with col1:
    min_pe = st.sidebar.number_input("最小 PE", value=0.0, step=1.0)
with col2:
    max_pe = st.sidebar.number_input("最大 PE", value=500.0, step=1.0)

max_market_cap = st.sidebar.number_input("最大流通市值 (亿)", value=5000.0, step=10.0)

# 股票代码搜索
search_code = st.sidebar.text_input("搜索股票代码 (如 000001)", help="留空显示全场")

# ========== 数据查询 ==========

# 构建查询逻辑
def fetch_filtered_data():
    conditions = ["日期 = ?"]
    params = [selected_date]
    
    if search_code:
        conditions.append("代码 LIKE ?")
        params.append(f"%{search_code}%")
    else:
        conditions.append("涨跌幅 >= ?")
        params.append(min_pct_chg)
        conditions.append("涨跌幅 <= ?")
        params.append(max_pct_chg)
        
        if min_pe > 0:
            conditions.append("PE >= ?")
            params.append(min_pe)
        if max_pe < 500:
            conditions.append("PE <= ?")
            params.append(max_pe)
            
        if max_market_cap < 5000:
            conditions.append("流通市值 <= ?")
            params.append(max_market_cap * 10000) # 亿 -> 万
            
    where_clause = " AND ".join(conditions)
    sql = f"SELECT * FROM daily_data WHERE {where_clause} ORDER BY 涨跌幅 DESC"
    
    return sync_service.query(sql, tuple(params))

# 执行查询
with st.spinner("查询中..."):
    df = fetch_filtered_data()

# ========== 数据展示 ==========

if df.empty:
    st.info(f"🧐 未找到匹配 '{selected_date}' 的数据，请尝试调整筛选条件。")
else:
    # 统计信息
    st.markdown(f"**共找到 {len(df)} 条记录**")
    
    # 分页设置
    items_per_page = 50
    total_pages = (len(df) - 1) // items_per_page + 1
    
    if total_pages > 1:
        page_num = st.number_input(f"页码 (1/{total_pages})", min_value=1, max_value=total_pages, value=1)
    else:
        page_num = 1
        
    start_idx = (page_num - 1) * items_per_page
    end_idx = start_idx + items_per_page
    
    page_df = df.iloc[start_idx:end_idx].copy()
    
    # 指标美化
    if '流通市值' in page_df.columns:
        page_df['流通市值(亿)'] = (page_df['流通市值'] / 10000).round(2)
    if '总市值' in page_df.columns:
        page_df['总市值(亿)'] = (page_df['总市值'] / 10000).round(2)
        
    # 重新排列列，方便查看
    display_cols = ['日期', '代码', '收盘', '涨跌幅', '换手率', 'PE', 'PE_TTM', 'PB', '流通市值(亿)', '成交额']
    available_cols = [c for c in display_cols if c in page_df.columns]
    
    # 重点显示
    st.dataframe(
        page_df[available_cols],
        use_container_width=True,
        hide_index=True,
        column_config={
            "日期": st.column_config.TextColumn("日期"),
            "代码": st.column_config.TextColumn("代码"),
            "涨跌幅": st.column_config.NumberColumn("涨跌幅", format="%.2f%%"),
            "换手率": st.column_config.NumberColumn("换手率", format="%.2f%%"),
            "流通市值(亿)": st.column_config.NumberColumn("流通市值(亿)", format="%.2f 亿"),
            "成交额": st.column_config.NumberColumn("成交额", format="%.0f")
        }
    )
    
    # 导出功能
    st.markdown("---")
    csv = df.to_csv(index=False).encode('utf-8-sig')
    st.download_button(
        label="📥 导出当前筛选结果为 CSV",
        data=csv,
        file_name=f"stock_data_{selected_date}.csv",
        mime="text/csv",
    )

# ========== 底部信息 ==========
st.markdown("---")
st.caption(f"💡 数据源: Tushare Pro | 数据库文件: {sync_service.db_path}")
