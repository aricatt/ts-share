"""
数据浏览器页面
展示本地 SQLite 数据库中的同步数据，支持多维度查询：
1. 全市场快照 (截面数据)
2. 个股历史 K 线 (时序数据)
"""
import streamlit as st
import pandas as pd
import sys
import os

sys.path.insert(0, '.')
from services import DataSyncService, StockService
from components.charts import render_chart, create_kline_chart
from components.stock_details import show_stock_details

# 页面配置
st.set_page_config(page_title="数据浏览器 - TS-Share", page_icon="📁", layout="wide")

# 初始化服务
sync_service = DataSyncService()
stock_service = StockService(use_cache=True)

# 检查数据库
if not os.path.exists(sync_service.db_path):
    st.warning("⚠️ 数据库文件不存在，请先前往「系统设置」同步数据。")
    if st.button("前往同步"):
        st.switch_page("pages/3_settings.py")
    st.stop()

# ========== 页面标题与模式切换 ==========
st.title("📁 数据浏览器")

view_mode = st.radio(
    "选择浏览模式",
    options=["全市场快照", "个股历史查询"],
    horizontal=True,
    help="「全市场快照」查看某一日全市场的行情指标；「个股历史查询」查看单只股票的时间序列数据和 K 线图。"
)

st.markdown("---")

# 获取已同步的日期列表
synced_dates = sync_service.get_synced_dates()
if not synced_dates:
    st.warning("📭 数据库为空，请先同步数据。")
    st.stop()

# ==================== 模式 1：全市场快照 ====================
if view_mode == "全市场快照":
    # 侧边栏筛选器
    st.sidebar.header("🔍 全市场筛选")
    
    selected_date = st.sidebar.selectbox("选择日期", options=reversed(synced_dates), index=0)
    
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
    search_code = st.sidebar.text_input("搜索股票代码 (如 000001)", help="留空显示全场")

    # 构建查询逻辑
    def fetch_market_data():
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
        sql = f"""
            SELECT d.*, b.名称 
            FROM daily_data d
            LEFT JOIN stock_basic b ON d.代码 = b.代码
            WHERE {where_clause.replace('日期', 'd.日期').replace('代码', 'd.代码').replace('涨跌幅', 'd.涨跌幅').replace('PE', 'd.PE').replace('流通市值', 'd.流通市值')}
            ORDER BY d.涨跌幅 DESC
        """
        return sync_service.query(sql, tuple(params))

    # 执行查询
    with st.spinner("查询中..."):
        df = fetch_market_data()
        if not df.empty:
            df = df.loc[:, ~df.columns.duplicated()]

    if df.empty:
        st.info(f"🧐 未找到匹配 '{selected_date}' 的数据。")
    else:
        st.subheader(f"📊 全市场快照 - {selected_date}")
        st.markdown(f"**共找到 {len(df)} 条记录**")
        
        # 分页设置
        items_per_page = 50
        total_pages = (len(df) - 1) // items_per_page + 1
        page_num = st.number_input(f"页码 (1/{total_pages})", min_value=1, max_value=total_pages, value=1) if total_pages > 1 else 1
            
        start_idx = (page_num - 1) * items_per_page
        end_idx = start_idx + items_per_page
        page_df = df.iloc[start_idx:end_idx].copy()
        
        # 指标美化
        if '流通市值' in page_df.columns:
            page_df['流通市值(亿)'] = (page_df['流通市值'] / 10000).round(2)
            
        display_cols = ['日期', '代码', '名称', '收盘', '涨跌幅', '换手率', 'PE', 'PE_TTM', 'PB', '流通市值(亿)', '成交额']
        available_cols = [c for c in display_cols if c in page_df.columns]
        
        st.dataframe(
            page_df[available_cols],
            use_container_width=True,
            hide_index=True,
            column_config={
                "代码": st.column_config.TextColumn("代码"),
                "名称": st.column_config.TextColumn("名称"),
                "涨跌幅": st.column_config.NumberColumn("涨跌幅", format="%.2f%%"),
                "换手率": st.column_config.NumberColumn("换手率", format="%.2f%%"),
                "流通市值(亿)": st.column_config.NumberColumn("流通市值(亿)", format="%.2f 亿"),
            }
        )
        
        # 导出
        csv = df.to_csv(index=False).encode('utf-8-sig')
        st.download_button(label="📥 导出筛选结果 CSV", data=csv, file_name=f"market_{selected_date}.csv", mime="text/csv")

# ==================== 模式 2：个股历史查询 ====================
else:
    st.sidebar.header("🔍 个股历史筛选")
    
    # 股票搜寻
    search_keyword = st.sidebar.text_input("搜索股票 (代码或名称)", value="000001", key="stock_search_input", help="支持代码模糊或名称模糊查询，如：'万科' 或 '000002'")
    
    # 执行搜索
    search_results = stock_service.search_stocks(search_keyword, limit=20)
    
    if search_results.empty:
        st.sidebar.error("🚫 未搜到相关股票")
        st.stop()
        
    # 构造更稳定的选项列表
    # 使用 字典 来存储 代码 -> 显示文字 的映射，方便反查
    stock_options = {}
    for _, row in search_results.iterrows():
        label = f"{row['代码']} - {row['名称']}"
        if pd.notnull(row['行业']):
            label += f" ({row['行业']})"
        stock_options[row['代码']] = label
    
    # 获取选项列表
    labels = list(stock_options.values())
    
    # 查找当前选中项在列表中的索引
    current_idx = 0
    if 'last_selected_code' in st.session_state:
        target_code = st.session_state['last_selected_code']
        # 如果当前搜索结果里包含上次选中的代码，则保持选中
        for i, code in enumerate(stock_options.keys()):
            if code == target_code:
                current_idx = i
                break
    
    selected_label = st.sidebar.selectbox(
        "选择匹配结果", 
        options=labels, 
        index=current_idx,
        key="stock_search_select"
    )
    
    # 解析选中的代码并存入 state
    search_code = selected_label.split(" - ")[0]
    st.session_state['last_selected_code'] = search_code
    selected_option = selected_label # 用于后续展示
    
    # 查看天数（默认显示1年约250个交易日，最大支持约3年）
    history_days = st.sidebar.slider("查看天数 (交易日)", min_value=5, max_value=750, value=250)
    
    # 复权选项
    adj_type = st.sidebar.selectbox("复权方式", options=["前复权", "未复权"])
    
    # 查询
    with st.spinner(f"正在获取 {selected_option} 的历史数据..."):
        df_history = stock_service.get_history(search_code, days=history_days)
        # 移除重复列名
        if df_history is not None and not df_history.empty:
            df_history = df_history.loc[:, ~df_history.columns.duplicated()]
        
    if df_history is None or df_history.empty:
        st.warning(f"⚠️ 数据库中未找到代码为 '{search_code}' 的历史数据。")
        st.info("💡 请确保已在设置中同步了该股票所属的时间范围。")
    else:
        # 准备图表数据
        df_plot = df_history.copy()
        if adj_type == "前复权" and "qfq_收盘" in df_plot.columns and df_plot["qfq_收盘"].notnull().any():
            df_plot["开盘"] = df_plot["qfq_开盘"]
            df_plot["最高"] = df_plot["qfq_最高"]
            df_plot["最低"] = df_plot["qfq_最低"]
            df_plot["收盘"] = df_plot["qfq_收盘"]
            chart_title = f"{search_code} {adj_type} K 线"
        else:
            chart_title = f"{search_code} 未复权 K 线"

        st.subheader(f"📈 {search_code} 历史行情与 K 线图 ({adj_type})")
        
        # 统计指标
        latest = df_history.iloc[-1]
        cols = st.columns(5)
        # 增加 AI 诊断按钮
        if cols[0].button("🔬 深度 AI 诊断", key=f"ai_diag_{search_code}", use_container_width=True):
            st.session_state.pending_details = (search_code, stock_service.get_stock_name(search_code))

        cols[1].metric("最新收盘", f"{latest['收盘']:.2f}")
        cols[2].metric("涨跌幅", f"{latest['涨跌幅']:.2f}%")
        cols[3].metric("最新成交", f"{int(latest['成交量']):,}")
        cols[4].metric("VMA5", f"{int(latest['vma5']):,}" if 'vma5' in latest and pd.notnull(latest['vma5']) else "N/A")
        
        st.markdown("---")
        
        # K 线图
        with st.container():
            kline_chart = create_kline_chart(df_plot, title=chart_title)
            render_chart(kline_chart, height=650)
            
        st.markdown("---")
        
        # 历史数据表格
        st.subheader("📋 历史数据明细")
        page_df = df_history.sort_values('日期', ascending=False).copy()
        
        if '流通市值' in page_df.columns:
            page_df['流通市值(亿)'] = (page_df['流通市值'] / 10000).round(2)
            
        display_cols = ['日期', '代码', '名称', '开盘', '最高', '最低', '收盘', '涨跌幅', '换手率', 'PE', '流通市值(亿)', '成交额']
        available_cols = [c for c in display_cols if c in page_df.columns]
        
        st.dataframe(
            page_df[available_cols],
            use_container_width=True,
            hide_index=True,
            column_config={
                "代码": st.column_config.TextColumn("代码"),
                "名称": st.column_config.TextColumn("名称"),
                "涨跌幅": st.column_config.NumberColumn("涨跌幅", format="%.2f%%"),
                "换手率": st.column_config.NumberColumn("换手率", format="%.2f%%"),
                "流通市值(亿)": st.column_config.NumberColumn("流通市值(亿)", format="%.2f 亿"),
            }
        )
        
        # 导出
        csv = df_history.to_csv(index=False).encode('utf-8-sig')
        st.download_button(label=f"📥 导出 {search_code} 历史数据 CSV", data=csv, file_name=f"stock_{search_code}_history.csv", mime="text/csv")

# ========== 底部信息 ==========
# ### 页面底部：处理待触发的弹窗 ###
if 'pending_details' not in st.session_state:
    st.session_state.pending_details = None

if st.session_state.pending_details:
    code, name = st.session_state.pending_details
    st.session_state.pending_details = None # 清除信号
    show_stock_details(code, name, stock_service)

st.markdown("---")
st.caption(f"💡 数据源: Tushare Pro | 数据库文件: {sync_service.db_path}")
