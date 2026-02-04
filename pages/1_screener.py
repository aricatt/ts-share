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
from config import MARKET_CAP_UNIT

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
            min_value=float(config.get('min', 0.0)),
            max_value=float(config.get('max', 100.0)),
            value=float(config['value']),
            step=float(config.get('step', 0.1)),
            key=f"param_{key}"
        )
    elif config['type'] == 'bool':
        adjusted_params[key] = st.sidebar.checkbox(
            config['label'],
            value=config['value'],
            key=f"param_{key}"
        )
    elif config['type'] == 'select':
        adjusted_params[key] = st.sidebar.selectbox(
            config['label'],
            options=config['options'],
            index=config['options'].index(config['value']) if config['value'] in config['options'] else 0,
            key=f"param_{key}"
        )
    elif config['type'] == 'list':
        adjusted_params[key] = st.sidebar.multiselect(
            config['label'],
            options=config.get('options', config['value']),
            default=config['value'],
            key=f"param_{key}"
        )

# 初始化 session_state
if 'screener_results' not in st.session_state:
    st.session_state.screener_results = None
if 'table_version' not in st.session_state:
    st.session_state.table_version = 0
if 'pending_details' not in st.session_state:
    st.session_state.pending_details = None

def reset_table_selection():
    """通过微调表格 key 来重置选中状态"""
    st.session_state.table_version += 1

# ========== 收藏功能模块 ==========

# 显示当前策略的收藏列表
def display_collections(rule_name):
    fav_df = stock_service.get_collected_stocks(rule_name)
    if not fav_df.empty:
        with st.expander(f"⭐ 我的收藏 - {rule_name} ({len(fav_df)} 只)", expanded=True):
            # 格式化展示数据
            disp_fav = fav_df.copy()
            if '涨跌幅' in disp_fav.columns:
                disp_fav['涨跌幅'] = disp_fav['涨跌幅'].astype(float).round(2).astype(str) + '%'
            if '总市值' in disp_fav.columns:
                disp_fav['市值(亿)'] = (disp_fav['总市值'] / 10000).round(2)
            
            show_cols = [c for c in ['代码', '名称', '涨跌幅', '市值(亿)', '行业', '收藏日期'] if c in disp_fav.columns]
            
            # 使用 unique key 避免冲突
            fav_selected = st.dataframe(
                disp_fav[show_cols],
                use_container_width=True,
                hide_index=True,
                on_select="rerun",
                selection_mode="single-row",
                key=f"fav_table_{rule_name}_v{st.session_state.table_version}"
            )
            
            if fav_selected and "rows" in fav_selected.selection and len(fav_selected.selection.rows) > 0:
                row_idx = fav_selected.selection.rows[0]
                sel_row = disp_fav.iloc[row_idx]
                # 记录待展示详情，增加版本号触发重载清空选中
                st.session_state.pending_details = (sel_row['代码'], sel_row['名称'])
                reset_table_selection()
                st.rerun()

# 处理收藏逻辑
def toggle_collection(code, name, rule_name):
    if stock_service.is_collected(code, rule_name):
        if stock_service.remove_collected_stock(code, rule_name):
            st.toast(f"已从【{rule_name}】中移除 {name}")
            return True
    else:
        if stock_service.collect_stock(code, name, rule_name):
            st.toast(f"已添加到【{rule_name}】收藏")
            return True
    return False

# 修订弹窗函数内容
@st.dialog("股票 K 线预览", width="large")
def show_stock_details(code, name):
    # 操作栏
    col1, col2 = st.columns([3, 1])
    with col1:
        st.subheader(f"{name} ({code})")
    with col2:
        is_fav = stock_service.is_collected(code, selected_rule)
        btn_label = "⭐ 取消收藏" if is_fav else "☆ 添加收藏"
        if st.button(btn_label, use_container_width=True, type="primary" if not is_fav else "secondary"):
            if toggle_collection(code, name, selected_rule):
                st.rerun()

    with st.spinner("获取历史行情中..."):
        df_hist = stock_service.get_history(code, days=120)
    
    if df_hist is not None and not df_hist.empty:
        kline_chart = create_kline_chart(df_hist, title=f"{name} - 最近半年走势")
        render_chart(kline_chart, height=500)
    else:
        st.warning("暂无历史行情数据可供预览")

# 先显示收藏夹
st.markdown("---")
display_collections(selected_rule)

# 开始筛选按钮逻辑 ... (保持原样，但确保 selected_rule 是一致的)
if st.sidebar.button("🚀 开始筛选", type="primary", use_container_width=True):
    with st.spinner("正在筛选股票..."):
        try:
            rule_instance = get_rule(selected_rule)
            for key, value in adjusted_params.items():
                if hasattr(rule_instance, key):
                    if key == 'max_market_cap': value = value * MARKET_CAP_UNIT
                    setattr(rule_instance, key, value)
            
            df = stock_service.get_data_by_source(rule_instance.data_source, date_str)
            if df.empty:
                st.warning("⚠️ 无法获取初始数据")
            else:
                total_before = len(df)
                if rule_instance.requires_history:
                    result = rule_instance.apply(df, history_provider=stock_service, date_str=date_str)
                else:
                    result = rule_instance.apply(df, date_str=date_str)
                
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

# 渲染筛选结果
if st.session_state.screener_results:
    res_data = st.session_state.screener_results
    # 增加校验：如果 session 中的策略和当前选中的不一致，不显示结果（或者提示）
    if res_data["rule_name"] == selected_rule:
        result = res_data["result"]
        
        st.info(f"📊 策略：{res_data['rule_name']} | 日期：{res_data['date_str']}")
        result_stats(result, res_data["total_before"])
        
        with st.expander("🔍 查看执行步骤跟踪", expanded=False):
            st.dataframe(res_data["tracker_df"], use_container_width=True, hide_index=True)
            st.code(res_data["tracker_summary"])

        st.markdown("---")
        if not result.empty:
            st.subheader("📋 筛选结果")
            
            display_df = result.copy()
            if '总市值' in display_df.columns:
                display_df['总市值(亿)'] = (display_df['总市值'] / 10000).round(2)
            
            for col in ['涨跌幅', '换手率']:
                if col in display_df.columns:
                    display_df[col] = display_df[col].round(2).astype(str) + '%'
            
            view_cols = [c for c in ['代码', '名称', '涨跌幅', '换手率', '总市值(亿)', '行业', '连板数'] if c in display_df.columns]
            display_view = display_df[view_cols]

            st.caption("💡 提示：点击行查看 K 线，并在弹窗内收藏关注。")
            
            selected = st.dataframe(
                display_view,
                use_container_width=True,
                hide_index=True,
                on_select="rerun",
                selection_mode="single-row",
                key=f"screener_result_table_v{st.session_state.table_version}"
            )

            if selected and "rows" in selected.selection and len(selected.selection.rows) > 0:
                row_idx = selected.selection.rows[0]
                sel_row = display_view.iloc[row_idx]
                # 记录待展示详情，增加版本号触发重载清空选中
                st.session_state.pending_details = (sel_row['代码'], sel_row['名称'])
                reset_table_selection()
                st.rerun()

        # 图表分析
        st.markdown("---")
        st.subheader("📊 数据可视化分析")
        
        # 1. 行业分布 (单行全宽)
        if '行业' in result.columns:
            st.markdown("#### 🥧 行业分布概览")
            render_chart(create_industry_pie(result), height=500)
            
        st.markdown("<br>", unsafe_allow_html=True)
            
        # 2. 换手率排行 (单行全宽)
        st.markdown("#### 📊 换手率 TOP 20")
        render_chart(create_turnover_bar(result, top_n=20), height=500)
    else:
        st.info("💡 请点击侧边栏的「开始筛选」来获取最新策略结果")

# 使用说明
st.sidebar.markdown("---")
st.sidebar.markdown("""
### 📖 使用说明
1. 选择策略，上方自动显示已收藏该策略的股票
2. 调整参数并点击「开始筛选」
3. 点击结果行查看详情并支持收藏
""")

# ### 页面底部：处理待触发的弹窗 ###
if st.session_state.pending_details:
    code, name = st.session_state.pending_details
    st.session_state.pending_details = None # 清除信号，防止循环
    show_stock_details(code, name)
