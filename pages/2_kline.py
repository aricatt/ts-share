"""
K线分析页面
"""
import streamlit as st
from datetime import datetime, timedelta

from services import StockService
from components.charts import render_chart, create_kline_chart

# 页面配置
st.set_page_config(page_title="K线分析 - TS-Share", page_icon="📈", layout="wide")

st.title("📈 K线分析")
st.markdown("查看个股历史K线和技术指标")

# 初始化服务
stock_service = StockService(use_cache=True)

# 侧边栏
st.sidebar.header("🔍 查询参数")

# 股票代码输入
stock_code = st.sidebar.text_input("股票代码", value="000001", max_chars=6)

# 查询天数
days = st.sidebar.slider("查询天数", min_value=30, max_value=365, value=120)

# 查询按钮
if st.sidebar.button("🔍 查询K线", type="primary", use_container_width=True):
    with st.spinner(f"正在获取 {stock_code} 的K线数据..."):
        try:
            df = stock_service.get_history(stock_code, days=days)
            
            if df is None or df.empty:
                st.warning(f"⚠️ 未找到 {stock_code} 的K线数据")
            else:
                # 基本信息
                st.subheader(f"📊 {stock_code} K线图")
                
                # 统计信息
                col1, col2, col3, col4 = st.columns(4)
                latest = df.iloc[-1]
                
                with col1:
                    st.metric("最新收盘", f"{latest['收盘']:.2f}")
                with col2:
                    st.metric("最新涨跌", f"{latest['涨跌幅']:.2f}%")
                with col3:
                    st.metric("最高价", f"{df['最高'].max():.2f}")
                with col4:
                    st.metric("最低价", f"{df['最低'].min():.2f}")
                
                st.markdown("---")
                
                # K线图
                render_chart(create_kline_chart(df, title=f"{stock_code} K线图"), height=550)
                
                # 原始数据
                with st.expander("📋 查看原始数据"):
                    st.dataframe(df.tail(30), use_container_width=True)
                    
        except Exception as e:
            st.error(f"❌ 查询失败: {str(e)}")

# 使用说明
st.sidebar.markdown("---")
st.sidebar.markdown("""
### 📖 使用说明
1. 输入6位股票代码
2. 选择查询天数
3. 点击"查询K线"
""")

st.sidebar.markdown("### 💡 常用代码")
st.sidebar.markdown("""
- 000001 平安银行
- 600519 贵州茅台
- 000858 五粮液
""")
