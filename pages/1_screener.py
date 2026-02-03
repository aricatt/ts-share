"""
选股器页面
"""
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

from services import StockService
from rules import get_rule, get_all_rules
from components.charts import render_chart, create_industry_pie, create_turnover_bar, create_market_cap_bar
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

# 开始筛选按钮
if st.sidebar.button("🚀 开始筛选", type="primary", use_container_width=True):
    # 创建策略实例（使用调整后的参数）
    rule_instance = get_rule(selected_rule)
    for key, value in adjusted_params.items():
        if hasattr(rule_instance, key):
            # 特殊处理：max_market_cap 需要从亿转换为元
            if key == 'max_market_cap':
                value = value * 1e8
            setattr(rule_instance, key, value)
    
    # 显示数据源信息
    data_source = rule_instance.data_source
    if data_source == "zt_pool":
        st.info(f"📊 数据源：涨停股池 ({date_str})")
    elif data_source == "historical_zt":
        st.info("📊 数据源：历史涨停股池（过去90天曾涨停的股票）")
    else:
        st.info("📊 数据源：全A股实时行情")
    
    with st.spinner("正在获取数据..."):
        try:
            # 根据策略的数据源类型获取数据
            df = stock_service.get_data_by_source(data_source, date_str)
            
            if df.empty:
                st.warning(f"⚠️ 无法获取数据（可能是非交易日或网络问题）")
            else:
                total_before = len(df)
                
                # 应用策略
                if rule_instance.requires_history:
                    # 需要历史数据的策略
                    result = rule_instance.apply(df, history_provider=stock_service, date_str=date_str)
                else:
                    result = rule_instance.apply(df, date_str=date_str)
                
                # 显示统计
                st.markdown("---")
                result_stats(result, total_before)
                
                # ========== 步骤跟踪 ==========
                st.markdown("---")
                st.subheader("🔍 执行步骤跟踪")
                
                tracker = rule_instance.get_tracker()
                if tracker.steps:
                    # 显示步骤表格
                    step_df = tracker.to_dataframe()
                    
                    # 添加颜色标记
                    def highlight_filtered(row):
                        if row['过滤数'] > 0:
                            return ['background-color: #ffebee'] * len(row)
                        return ['background-color: #e8f5e9'] * len(row)
                    
                    st.dataframe(
                        step_df.style.apply(highlight_filtered, axis=1),
                        use_container_width=True,
                        hide_index=True
                    )
                    
                    # 显示文字摘要
                    with st.expander("📋 详细执行日志"):
                        st.code(tracker.get_summary())
                else:
                    st.info("该策略未提供步骤跟踪信息")
                # ========== 步骤跟踪结束 ==========
                
                st.markdown("---")
                
                if not result.empty:
                    # 数据表格
                    st.subheader("📋 筛选结果")
                    
                    # 根据数据源选择显示列（不同数据源列名可能不同）
                    available_cols = result.columns.tolist()
                    
                    # 通用显示列
                    display_cols = []
                    col_mapping = {
                        '代码': '代码',
                        '名称': '名称', 
                        '涨跌幅': '涨跌幅',
                        '换手率': '换手率',
                        '总市值': '总市值',
                    }
                    
                    # 涨停股池特有列
                    if '连板数' in available_cols:
                        col_mapping['连板数'] = '连板数'
                    if '所属行业' in available_cols:
                        col_mapping['所属行业'] = '所属行业'
                    
                    # 筛选存在的列
                    for col in col_mapping.keys():
                        if col in available_cols:
                            display_cols.append(col)
                    
                    display_df = result[display_cols].copy()
                    
                    # 格式化列
                    if '总市值' in display_df.columns:
                        display_df['总市值(亿)'] = (display_df['总市值'] / 10000).round(2)
                        display_df = display_df.drop('总市值', axis=1)
                    if '涨跌幅' in display_df.columns:
                        display_df['涨跌幅'] = display_df['涨跌幅'].round(2).astype(str) + '%'
                    if '换手率' in display_df.columns:
                        display_df['换手率'] = display_df['换手率'].round(2).astype(str) + '%'
                    
                    st.dataframe(display_df, use_container_width=True, hide_index=True)
                    
                    # 图表分析
                    st.markdown("---")
                    st.subheader("📊 数据可视化")
                    
                    # 仅在有相应数据时显示图表
                    if '所属行业' in available_cols:
                        chart_col1, chart_col2 = st.columns(2)
                        
                        with chart_col1:
                            st.markdown("#### 🥧 行业分布")
                            render_chart(create_industry_pie(result), height=450)
                        
                        with chart_col2:
                            st.markdown("#### 📊 换手率 TOP 10")
                            render_chart(create_turnover_bar(result), height=450)
                        
                        st.markdown("#### 💰 市值分布（最小 10 只）")
                        render_chart(create_market_cap_bar(result), height=400)
                    else:
                        st.markdown("#### 📊 换手率 TOP 10")
                        render_chart(create_turnover_bar(result), height=450)
                        
                        st.markdown("#### 💰 市值分布（最小 10 只）")
                        render_chart(create_market_cap_bar(result), height=400)
                    
                else:
                    st.info("❌ 没有符合条件的股票")
                    
        except Exception as e:
            st.error(f"❌ 筛选失败: {str(e)}")
            import traceback
            st.code(traceback.format_exc())

# 使用说明
st.sidebar.markdown("---")
st.sidebar.markdown("""
### 📖 使用说明
1. 选择日期（默认昨天）
2. 选择策略
3. 调整参数（可选）
4. 点击"开始筛选"
""")
