"""
TS-Share 涨停板选股器
主入口文件
"""
import streamlit as st

# 页面配置
st.set_page_config(
    page_title="TS-Share 选股器",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# 自定义样式
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0;
    }
    .sub-header {
        color: #888;
        margin-top: 0;
        margin-bottom: 2rem;
    }
</style>
""", unsafe_allow_html=True)

# 主页内容
st.markdown('<p class="main-header">📈 TS-Share 选股器</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">基于 Streamlit + Tushare Pro + SQLite 构建</p>', unsafe_allow_html=True)

st.markdown("---")

# 功能介绍
col1, col2 = st.columns(2)

with col1:
    st.markdown("### 📊 选股器")
    st.markdown("""
    - 多种选股策略（涨停池、龙回头等）
    - 结合 Tushare Pro 实时与历史数据
    - 快速定位市场热点
    """)
    st.page_link("pages/1_screener.py", label="进入选股器", icon="🚀")

with col2:
    st.markdown("### 📉 K线分析")
    st.markdown("""
    - 专业 PyEcharts K 线图表
    - 个股历史全貌展示
    - 成交量与资金指标分析
    """)
    st.page_link("pages/2_kline.py", label="进入K线分析", icon="📈")

st.markdown("---")

col3, col4 = st.columns(2)

with col3:
    st.markdown("### 📁 数据浏览器")
    st.markdown("""
    - 查看本地 SQLite 数据库内容
    - 支持全市场筛选与导出
    - 本地同步数据的可视化管理
    """)
    st.page_link("pages/4_data_browser.py", label="进入数据浏览器", icon="📁")

with col4:
    st.markdown("### ⚙️ 系统设置")
    st.markdown("""
    - **Tushare Pro 数据同步**
    - 缓存与数据库维护
    - Token 配置检查
    """)
    st.page_link("pages/3_settings.py", label="进入设置", icon="🔧")

st.markdown("---")

# 快速开始
st.markdown("### 🚀 快速开始")
st.markdown("""
1. 首先前往 **⚙️ 系统设置** 检查 Token 并同步历史数据。
2. 使用 **📁 数据浏览器** 确认同步结果（支持分页与筛选）。
3. 进入 **📊 选股器** 选择策略进行市场分析。
4. 点击筛选结果中的代码可跳转到 **📉 K线分析** 查看详情。
""")

# 侧边栏
st.sidebar.markdown("### 📌 当前版本")
st.sidebar.info("v2.0.0 (SQLite Edition)")

st.sidebar.markdown("### 📚 核心价值")
st.sidebar.markdown("""
- **快**: 本地查询，毫秒级响应
- **准**: Tushare Pro 专业数据源
- **便**: 单文件 SQLite 管理
""")
