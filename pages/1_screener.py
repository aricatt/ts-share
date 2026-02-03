"""
选股器页面
"""
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

from services import StockService
from rules import get_rule, get_all_rules
from components.charts import render_chart, create_industry_pie, create_turnover_bar, create_market_cap_bar, create_kline_chart
from components.widgets import result_stats

# 页面配置
st.set_page_config(page_title="选股器 - TS-Share", page_icon="📊", layout="wide")

st.title("📊 选股器")
st.markdown("根据策略筛选符合条件的股票")

# 初始化服务
stock_service = StockService(use_cache=True)

# 侧边栏 - 参数设置
st.sidebar.header("🔍 筛选参数")

# 日期选择
default_date = datetime.now() - timedelta(days=1)
selected_date = st.sidebar.date_input("选择日期", value=default_date)
date_str = selected_date.strftime("%Y%m%d")

# 策略选择
rule_names = get_all_rules()
selected_rule = st.sidebar.selectbox("选择策略", options=rule_names)

# 获取策略实例并显示参数
rule = get_rule(selected_rule)
st.sidebar.caption(f"📝 {rule.description}")

# 策略参数调整（可选）
st.sidebar.markdown("---")
st.sidebar.markdown("### ⚙️ 参数调整")

# 根据策略动态显示参数
params = rule.get_params()
adjusted_params = {}

for key, config in params.items():
    if config['type'] == 'float':
        adjusted_params[key] = st.sidebar.slider(
            config['label'],
            min_value=0.0,
            max_value=100.0,
            value=float(config['value']),
            key=f"param_{key}"
        )
    elif config['type'] == 'bool':
        adjusted_params[key] = st.sidebar.checkbox(
            config['label'],
            value=config['value'],
            key=f"param_{key}"
        )

# 初始化 session_state 用于持久化筛选结果
if 'screener_results' not in st.session_state:
    st.session_state.screener_results = None

# 定义弹窗函数
@st.dialog("股票 K 线预览", width="large")
def show_stock_details(code, name):
    with st.spinner("获取历史行情中..."):
        df_hist = stock_service.get_history(code, days=120)
    if df_hist is not None and not df_hist.empty:
        kline_chart = create_kline_chart(df_hist, title=name)
        render_chart(kline_chart, height=500)
    else:
        st.warning("暂无历史行情数据可供预览")

# 开始筛选按钮
if st.sidebar.button("🚀 开始筛选", type="primary", use_container_width=True):
    with st.spinner("正在筛选股票..."):
        try:
            # 创建策略实例
            rule_instance = get_rule(selected_rule)
            for key, value in adjusted_params.items():
                if hasattr(rule_instance, key):
                    if key == 'max_market_cap': value = value * 1e8
                    setattr(rule_instance, key, value)
            
            # 获取数据并应用策略
            df = stock_service.get_data_by_source(rule_instance.data_source, date_str)
            if df.empty:
                st.warning("⚠️ 无法获取初始数据")
            else:
                total_before = len(df)
                if rule_instance.requires_history:
                    result = rule_instance.apply(df, history_provider=stock_service, date_str=date_str)
                else:
                    result = rule_instance.apply(df, date_str=date_str)
                
                # 保存到 session_state
                st.session_state.screener_results = {
                    "result": result,
                    "total_before": total_before,
                    "date_str": date_str,
                    "rule_name": selected_rule,
                    "tracker_df": rule_instance.get_tracker().to_dataframe(),
                    "tracker_summary": rule_instance.get_tracker().get_summary()
                }
        except Exception as e:
            st.error(f"❌ 筛选失败: {str(e)}")
            import traceback
            st.code(traceback.format_exc())

# 渲染筛选结果 (如果 session_state 中有数据)
if st.session_state.screener_results:
    res_data = st.session_state.screener_results
    result = res_data["result"]
    
    st.info(f"📊 策略：{res_data['rule_name']} | 日期：{res_data['date_str']}")
    result_stats(result, res_data["total_before"])
    
    # 步骤跟踪
    with st.expander("🔍 查看执行步骤跟踪", expanded=False):
        st.dataframe(res_data["tracker_df"], use_container_width=True, hide_index=True)
        st.code(res_data["tracker_summary"])

    st.markdown("---")
    if not result.empty:
        st.subheader("📋 筛选结果")
        
        # 准备显示数据
        display_df = result.copy()
        if '总市值' in display_df.columns:
            display_df['总市值(亿)'] = (display_df['总市值'] / 10000).round(2)
        
        # 统一格式化
        for col in ['涨跌幅', '换手率']:
            if col in display_df.columns:
                display_df[col] = display_df[col].round(2).astype(str) + '%'
        
        # 定义显示列
        view_cols = [c for c in ['代码', '名称', '涨跌幅', '换手率', '总市值(亿)', '所属行业', '连板数'] if c in display_df.columns]
        display_view = display_df[view_cols]

        st.caption("💡 提示：点击下方表格任意行，可弹出 120 日 K 线预览。")
        
        # 数据展示与交互
        selected = st.dataframe(
            display_view,
            use_container_width=True,
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row",
            key="screener_result_table"
        )

        # 触发弹窗逻辑
        if selected and "rows" in selected.selection and len(selected.selection.rows) > 0:
            row_idx = selected.selection.rows[0]
            sel_row = display_view.iloc[row_idx]
            show_stock_details(sel_row['代码'], sel_row['名称'])

        # 图表分析
        st.markdown("---")
        st.subheader("📊 数据可视化")
        chart_col1, chart_col2 = st.columns(2)
        with chart_col1:
            if '所属行业' in result.columns:
                st.markdown("#### 🥧 行业分布")
                render_chart(create_industry_pie(result), height=400)
        with chart_col2:
            st.markdown("#### 📊 换手率 TOP 10")
            render_chart(create_turnover_bar(result), height=400)
    else:
        st.warning("😵 选股完成，但没有股票符合条件")

# 使用说明
st.sidebar.markdown("---")
st.sidebar.markdown("""
### 📖 使用说明
1. 选择日期（默认昨天）
2. 选择策略
3. 调整参数（可选）
4. 点击"开始筛选"
""")
