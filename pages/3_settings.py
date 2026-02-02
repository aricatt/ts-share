"""
系统设置页面
"""
import streamlit as st
import os
from services import CacheService, DataSyncService
from config import CACHE_DIR

# 页面配置
st.set_page_config(page_title="设置 - TS-Share", page_icon="⚙️", layout="wide")

st.title("⚙️ 系统设置")
st.markdown("管理数据同步、缓存和系统配置")

# 初始化服务
cache_service = CacheService()
sync_service = DataSyncService()

# ========== 数据同步 ==========
st.markdown("---")
st.subheader("📥 本地数据同步")
st.markdown("将A股历史数据同步到本地（按股票代码分区），加速后续分析")

# 同步状态
status = sync_service.get_sync_status()

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("📊 已同步股票", status['total_stocks'])
with col2:
    st.metric("📅 历史天数", status['days'])
with col3:
    st.metric("💾 数据大小", f"{status['total_size_mb']} MB")
with col4:
    last_sync = status['last_sync']
    if last_sync:
        last_sync_short = last_sync[:10]
    else:
        last_sync_short = "未同步"
    st.metric("🕐 最后同步", last_sync_short)

# 日期范围
date_range = status.get('date_range', {})
if date_range.get('start') and date_range.get('end'):
    st.info(f"📆 数据范围: {date_range['start']} ~ {date_range['end']} (不含当日)")

# 同步按钮
st.markdown("---")
sync_col1, sync_col2, sync_col3 = st.columns(3)
# API 状态检测
st.markdown("---")
st.subheader("📡 数据源状态")
health_col1, health_col2 = st.columns([1, 3])
with health_col1:
    if st.button("🔍 检查 API 状态"):
        with st.spinner("正在检测..."):
            is_healthy = sync_service.check_api_health()
            if is_healthy:
                st.success("🟢 API 通畅")
            else:
                st.error("🔴 接口受限 (限流中)")

with health_col2:
    st.info("💡 如果同步一直失败，请点击左侧按钮检查是否被封 IP")

st.markdown("---")
sync_col1, sync_col2, sync_col3 = st.columns(3)

with sync_col1:
    days = st.slider("同步天数", min_value=30, max_value=365, value=120)

with sync_col2:
    # 修改默认值为 1，并在帮助中说明
    workers = st.slider("并发数", min_value=1, max_value=5, value=1, help="拉取历史数据建议使用 1，避免被封 IP")

with sync_col3:
    force_sync = st.checkbox("强制全量同步", value=False, help="忽略已有数据，重新下载所有数据")

if st.button("🚀 开始同步", type="primary", use_container_width=True):
    if force_sync:
        st.warning("⚠️ 强制全量同步，将重新下载所有数据")
    else:
        st.info("📊 增量同步模式，将跳过已有数据")
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    detail_status = st.empty()
    
    # 增加中断按钮
    if st.button("🛑 中断同步"):
        sync_service.request_stop()
        st.warning("正在请求中断，请稍候...")
    
    def update_progress(current, total, code, status_msg="同步中"):
        # 顺便检查一下是否有停止请求（防止 callback 丢失标志）
        progress = current / total
        progress_bar.progress(progress)
        status_text.text(f"当前进度: {current}/{total} - {code}")
        detail_status.info(f"状态: {status_msg}")
    
    with st.spinner("同步中..."):
        success = sync_service.sync_all_stocks(
            days=days,
            max_workers=workers,
            progress_callback=update_progress,
            force=force_sync
        )
    
    if success:
        st.success("✅ 数据同步完成！")
        st.rerun()
    else:
        st.error("❌ 同步中断，可能由于触发严重限流，请稍后重试")

# ========== 缓存管理 ==========
st.markdown("---")
st.subheader("💾 缓存管理")

# 缓存统计
cache_path = CACHE_DIR
if os.path.exists(cache_path):
    cache_files = [f for f in os.listdir(cache_path) if f.endswith('.parquet')]
    cache_size = sum(os.path.getsize(os.path.join(cache_path, f)) for f in os.listdir(cache_path) if os.path.isfile(os.path.join(cache_path, f)))
    cache_size_mb = cache_size / (1024 * 1024)
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("缓存文件数", len(cache_files))
    with col2:
        st.metric("缓存大小", f"{cache_size_mb:.2f} MB")
    
    # 缓存文件列表
    with st.expander("📋 查看缓存文件"):
        for f in cache_files[:20]:
            st.text(f)
        if len(cache_files) > 20:
            st.text(f"... 还有 {len(cache_files) - 20} 个文件")
else:
    st.info("📭 暂无缓存文件")

# 清理缓存按钮
if st.button("🗑️ 清空所有缓存", type="secondary"):
    if cache_service.clear_all():
        st.success("✅ 缓存已清空")
        st.rerun()
    else:
        st.error("❌ 清空缓存失败")

# ========== 系统信息 ==========
st.markdown("---")
st.subheader("ℹ️ 系统信息")

import akshare as ak
import streamlit

info_col1, info_col2 = st.columns(2)

with info_col1:
    st.markdown("**依赖版本**")
    st.text(f"AkShare: {ak.__version__}")
    st.text(f"Streamlit: {streamlit.__version__}")

with info_col2:
    st.markdown("**项目信息**")
    st.text("TS-Share v1.0.0")
    st.text("Python 股票选股器")

st.markdown("---")

# 关于
st.subheader("📖 关于")
st.markdown("""
**TS-Share** 是一个基于 Python 的 A 股选股工具，具有以下特点：

- 🚀 基于 Streamlit 快速构建
- 📊 使用 AkShare 获取免费股票数据
- 📈 PyEcharts 专业图表可视化
- 💾 本地数据同步，高效分析
- 🔧 模块化架构，易于扩展

**技术栈**: Streamlit + AkShare + PyEcharts + Pandas + Parquet
""")
