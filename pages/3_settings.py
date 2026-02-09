"""
系统设置页面 - 数据同步管理
"""
import streamlit as st
import os
import time
from datetime import datetime, timedelta

import sys
sys.path.insert(0, '.')

from services import CacheService, DataSyncService
from config import CACHE_DIR, TUSHARE_TOKEN

# 页面配置
st.set_page_config(page_title="设置 - TS-Share", page_icon="⚙️", layout="wide")

st.title("⚙️ 系统设置")
st.markdown("管理数据同步、缓存和系统配置")

# 初始化服务
cache_service = CacheService()
sync_service = DataSyncService()

# ========== Token 状态 ==========
st.markdown("---")
st.subheader("🔑 Tushare Pro 配置")

if TUSHARE_TOKEN:
    st.success(f"✅ Token 已配置 (前8位: {TUSHARE_TOKEN[:8]}...)")
else:
    st.error("❌ Token 未配置，请在 config.py 中设置 TUSHARE_TOKEN")
    st.stop()

# ========== 数据同步状态 ==========
st.markdown("---")
st.subheader("📊 数据同步状态")

status = sync_service.get_sync_status_info()

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("📁 股票数量", f"{status['total_stocks']:,}")
with col2:
    st.metric("📝 记录总数", f"{status['total_records']:,}")
with col3:
    st.metric("💾 数据库大小", f"{status['db_size_mb']} MB")
with col4:
    last_sync = status.get('last_sync')
    if last_sync:
        last_sync_short = last_sync[:10]
    else:
        last_sync_short = "未同步"
    st.metric("🕐 最后同步", last_sync_short)

# 日期范围
date_range = status.get('date_range', {})
if date_range.get('start') and date_range.get('end'):
    st.info(f"📆 数据范围: {date_range['start']} ~ {date_range['end']}")

# ========== 同步控制 ==========
st.markdown("---")
st.subheader("📥 数据同步")
st.markdown("从 Tushare Pro 同步 A 股历史数据到本地 SQLite 数据库")

# 股票基础信息同步
sync_basic_col1, sync_basic_col2 = st.columns([1, 1])
with sync_basic_col1:
    if st.button("📋 同步股票基础信息", help="同步股票代码与名称的对应关系（仅需偶尔同步一次）"):
        with st.spinner("同步中..."):
            if sync_service.sync_stock_basic():
                st.success("✅ 股票基础信息同步成功")
                time.sleep(1)
                st.rerun()
            else:
                st.error("❌ 同步失败")

st.markdown("---")
col1, col2 = st.columns(2)
with col1:
    today = datetime.now()
    default_start = today - timedelta(days=120)
    start_date_val = st.date_input("开始日期", value=default_start)
with col2:
    end_date_val = st.date_input("结束日期", value=today)

force_sync = st.checkbox("强制全量同步", value=False, 
                        help="清空现有数据，重新同步所有数据")

st.caption("💡 采用「按日期批量获取」策略，120天数据约 2 分钟即可完成")

# 检查是否有同步任务在运行
current_sync_status = sync_service.get_sync_status()
if current_sync_status["is_syncing"]:
    st.warning(f"⚠️ 同步进行中... 已运行 {current_sync_status['elapsed_seconds']} 秒")
    if st.button("🛑 停止同步", type="secondary"):
        sync_service.request_stop()
        st.info("已发送停止请求，请稍候...")
        time.sleep(1)
        st.rerun()
else:
    # 开始同步按钮
    if st.button("🚀 开始同步", type="primary", use_container_width=True):
        st.markdown("---")
        st.subheader("📡 同步进度")
        
        # 进度显示元素
        stage_container = st.empty()
        progress_bar = st.progress(0)
        progress_text = st.empty()
        
        # 统计卡片
        stats_cols = st.columns(4)
        with stats_cols[0]:
            metric_total = st.empty()
        with stats_cols[1]:
            metric_current = st.empty()
        with stats_cols[2]:
            metric_records = st.empty()
        with stats_cols[3]:
            metric_eta = st.empty()
        
        # 当前日期
        current_date_container = st.empty()
        
        # 日志
        log_expander = st.expander("📋 详细日志", expanded=False)
        log_container = log_expander.empty()
        
        # 同步状态
        sync_state = {
            "start_time": time.time(),
            "total_records": 0,
            "logs": []
        }
        
        def update_progress(current, total, trade_date, status_msg):
            elapsed = time.time() - sync_state["start_time"]
            progress = current / total if total > 0 else 0
            
            # 解析记录数
            if "累计" in status_msg:
                try:
                    sync_state["total_records"] = int(status_msg.split("累计")[1].split("条")[0].strip())
                except:
                    pass
            
            # 预估时间
            if current > 0:
                avg_time = elapsed / current
                remaining = (total - current) * avg_time
                eta_str = str(timedelta(seconds=int(remaining)))
            else:
                eta_str = "计算中..."
            
            # 更新 UI
            if current < total:
                stage_container.info(f"📥 正在同步... ({current}/{total} 交易日)")
            else:
                stage_container.success("✅ 同步完成！")
            
            progress_bar.progress(progress)
            progress_text.markdown(f"**进度**: {current}/{total} ({progress*100:.0f}%)")
            
            metric_total.metric("📅 交易日", f"{total}")
            metric_current.metric("📍 当前", f"{current}/{total}")
            metric_records.metric("📝 记录", f"{sync_state['total_records']:,}")
            metric_eta.metric("⏱️ 剩余", eta_str)
            
            current_date_container.markdown(f"""
            <div style="padding: 10px; background-color: #1E1E1E; border-radius: 5px; margin: 10px 0;">
                <span style="color: #888;">当前日期:</span>
                <span style="color: #4CAF50; font-weight: bold; font-size: 1.2em;"> {trade_date}</span>
                <span style="color: #888; margin-left: 20px;">{status_msg}</span>
            </div>
            """, unsafe_allow_html=True)
            
            # 日志
            sync_state["logs"].append(f"[{current}/{total}] {trade_date}")
            if len(sync_state["logs"]) > 20:
                sync_state["logs"].pop(0)
            log_container.code("\n".join(sync_state["logs"]))
        
        # 执行同步
        stage_container.info("🔄 正在初始化...")
        
        success = sync_service.sync_all_stocks(
            start_date=start_date_val.strftime("%Y%m%d"),
            end_date=end_date_val.strftime("%Y%m%d"),
            progress_callback=update_progress,
            force=force_sync
        )
        
        if success:
            elapsed = time.time() - sync_state["start_time"]
            st.success(f"🎉 同步完成！共 {sync_state['total_records']:,} 条记录，耗时 {elapsed:.0f} 秒")
            st.balloons()
            time.sleep(2)
            st.rerun()
        else:
            st.error("❌ 同步中断")

# ========== 快速操作 ==========
st.markdown("---")
st.subheader("⚡ 快速操作")

quick_cols = st.columns(3)

with quick_cols[0]:
    if st.button("🔄 增量同步 (今日)", use_container_width=True):
        status_container = st.empty()
        p_bar = st.progress(0)
        
        def quick_progress(curr, total, date, msg):
            p_bar.progress(curr/total if total > 0 else 0)
            status_container.info(f"正在同步 {date}: {msg}")

        success = sync_service.sync_all_stocks(days=3, progress_callback=quick_progress)
        if success:
            status_container.success("✅ 增量同步完成")
            p_bar.empty()
            time.sleep(1)
            st.rerun()
        else:
            status_container.error("❌ 同步中断")

with quick_cols[1]:
    if st.button("📊 查看数据库", use_container_width=True):
        st.switch_page("pages/4_data_browser.py")

with quick_cols[2]:
    if st.button("🗑️ 清空数据库", type="secondary", use_container_width=True):
        if st.session_state.get("confirm_clear"):
            import sqlite3
            with sqlite3.connect(sync_service.db_path) as conn:
                conn.execute("DELETE FROM daily_data")
                conn.commit()
            st.success("✅ 数据库已清空")
            st.session_state["confirm_clear"] = False
            st.rerun()
        else:
            st.session_state["confirm_clear"] = True
            st.warning("⚠️ 再次点击确认清空")

# ========== 缓存管理 ==========
st.markdown("---")
st.subheader("💾 缓存管理")

# 1. 通用文件缓存
st.markdown("#### 📄 通用文件缓存")
cache_path = CACHE_DIR
if os.path.exists(cache_path):
    cache_files = [f for f in os.listdir(cache_path) if os.path.isfile(os.path.join(cache_path, f))]
    cache_size = sum(os.path.getsize(os.path.join(cache_path, f)) for f in cache_files)
    cache_size_mb = cache_size / (1024 * 1024)
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("缓存文件数", len(cache_files))
    with col2:
        st.metric("缓存大小", f"{cache_size_mb:.2f} MB")
    
    if st.button("🗑️ 清空文件缓存", type="secondary", key="clear_file_cache"):
        if cache_service.clear_all():
            st.success("✅ 文件缓存已清空")
            st.rerun()
else:
    st.info("📭 暂无文件缓存")

# 2. 分析数据缓存 (独立数据库)
st.markdown("#### 🧪 分析数据缓存 (需 Tushare 积分)")
from services import AnalysisCacheService
analysis_cache = AnalysisCacheService()
stats = analysis_cache.get_stats()

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("基本面记录", stats.get("fundamental_cache", 0))
with c2:
    st.metric("资金流记录", stats.get("money_flow_cache", 0))
with c4:
    st.metric("缓存库大小", f"{stats.get('db_size_mb', 0)} MB")

if st.button("🗑️ 清空分析缓存", type="secondary", key="clear_analysis_cache"):
    if analysis_cache.clear_all():
        st.success("✅ 分析缓存数据库已重置")
        st.rerun()

if st.button("🧹 清除已过期记录", key="clear_expired_cache"):
    count = analysis_cache.clear_expired()
    st.success(f"✅ 已清除 {count} 条过期记录")
    st.rerun()

# ========== 系统信息 ==========
st.markdown("---")
st.subheader("ℹ️ 系统信息")

import tushare as ts
import streamlit

col1, col2 = st.columns(2)

with col1:
    st.markdown("**依赖版本**")
    st.text(f"Tushare: {ts.__version__}")
    st.text(f"Streamlit: {streamlit.__version__}")

with col2:
    st.markdown("**项目信息**")
    st.text("TS-Share v2.0.0")
    st.text("存储: SQLite")
    st.text("数据源: Tushare Pro")

st.markdown("---")

# 关于
st.subheader("📖 关于")
st.markdown("""
**TS-Share** 是一个基于 Python 的 A 股选股工具：

- 🚀 基于 Streamlit 快速构建
- 📊 使用 **Tushare Pro** 获取专业股票数据
- 💾 **SQLite** 本地存储，支持 SQL 查询
- 📈 PyEcharts 专业图表可视化
- 🔧 模块化架构，易于扩展

**技术栈**: Streamlit + Tushare Pro + SQLite + PyEcharts
""")
