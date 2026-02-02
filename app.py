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
st.markdown('<p class="sub-header">基于 Streamlit + AkShare + PyEcharts 构建</p>', unsafe_allow_html=True)

st.markdown("---")

# 功能介绍
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("### 📊 选股器")
    st.markdown("""
    - 多种选股策略
    - 自定义筛选条件
    - 实时数据获取
    """)
    st.page_link("pages/1_screener.py", label="进入选股器", icon="🚀")

with col2:
    st.markdown("### 📈 K线分析")
    st.markdown("""
    - 专业 K 线图表
    - 技术指标叠加
    - 历史数据查询
    """)
    st.page_link("pages/2_kline.py", label="进入K线分析", icon="📉")

with col3:
    st.markdown("### ⚙️ 系统设置")
    st.markdown("""
    - 缓存管理
    - 参数配置
    - 数据维护
    """)
    st.page_link("pages/3_settings.py", label="进入设置", icon="🔧")

st.markdown("---")

# 快速开始
st.markdown("### 🚀 快速开始")
st.markdown("""
1. 点击左侧菜单 **📊 选股器** 进入选股页面
2. 选择日期和策略
3. 点击"开始筛选"按钮
4. 查看筛选结果和图表分析
""")

# 侧边栏
st.sidebar.markdown("### 📌 当前版本")
st.sidebar.info("v1.0.0")

st.sidebar.markdown("### 📚 策略列表")
st.sidebar.markdown("""
- 小盘涨停异动
- 龙回头
- 更多策略开发中...
""")
