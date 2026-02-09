import streamlit as st
import pandas as pd
from datetime import datetime
from services import StockService
from agents.analyst_agent import StockAnalystAgent
from components.charts import render_chart, create_kline_chart
from agents.config import LLM_CONFIG

@st.dialog("股票详情诊断", width="large")
def show_stock_details(code: str, name: str, stock_service: StockService):
    """
    显示股票详情弹窗
    """
    ts_code = stock_service._to_ts_code(code)
    
    # 标题栏
    st.subheader(f"🔍 {name} ({ts_code}) 详情诊断")
    
    # 准备五个标签页
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📈 K线走势", "📊 财务指标", "💰 资金流向", "📢 重要公告", "🤖 AI 智能诊断"])

    with tab1:
        # 默认显示1年 K 线
        df_hist = stock_service.get_history(code, days=250)
        if df_hist is not None and not df_hist.empty:
            chart = create_kline_chart(df_hist, title=f"{name} ({ts_code}) 历史K线")
            render_chart(chart, height=500)
        else:
            st.warning("暂无历史 K 线数据")

    with tab2:
        st.markdown("#### 📊 核心财务指标")
        fina = stock_service.get_fundamental(ts_code, 'fina_indicator')
        if fina is not None and not fina.empty:
            # 只显示最近一期的关键数据
            latest_fina = fina.iloc[0]
            col1, col2, col3 = st.columns(3)
            col1.metric("净利润率", f"{latest_fina.get('netprofit_margin', 0):.2f}%")
            col2.metric("ROE (净资产收益率)", f"{latest_fina.get('roe', 0):.2f}%")
            col3.metric("资产负债率", f"{latest_fina.get('debt_to_assets', 0):.2f}%")
            
            st.dataframe(fina.head(10), use_container_width=True)
        else:
            st.info("暂无历史财务指标数据")

    with tab3:
        st.markdown("#### 💰 资金流向 (近期)")
        last_date = datetime.now().strftime("%Y%m%d")
        money = stock_service.get_money_flow_cached(ts_code, last_date)
        if money is not None and not money.empty:
            st.dataframe(money.sort_values('trade_date', ascending=False), use_container_width=True)
        else:
            st.info("近期暂无资金流向数据")

    with tab4:
        st.markdown("#### 📢 重要公告与新闻 (最近30天)")
        news = stock_service.get_stock_news(ts_code)
        if news is not None and not news.empty:
            for _, item in news.iterrows():
                with st.container():
                    col_date, col_title = st.columns([1, 4])
                    col_date.caption(item['ann_date'])
                    col_title.markdown(f"**{item['title']}**")
                    if 'url' in item and item['url']:
                        with col_title:
                            st.markdown(f"[🔗 查看详情]({item['url']})")
                    st.divider()
        else:
            st.info("💡 最近 30 天暂无重要公告或权限受限")

    with tab5:
        st.markdown("#### 🤖 AutoGen 智能投研研讨会")
        st.info("AI 专家团将综合量价、财务、资金和新闻进行研讨，提供深度诊断。")
        
        # 1. 历史记录管理
        history = stock_service.analysis_cache.get_ai_analysis_history(ts_code)
        
        if history:
            h_col1, h_col2 = st.columns([4, 1])
            with h_col1:
                st.subheader("📝 历史分析记录")
            with h_col2:
                if st.button("🗑️ 清空所有历史", key=f"clear_all_{code}", use_container_width=True):
                    if stock_service.analysis_cache.clear_ai_analysis_history(ts_code):
                        st.success("历史记录已清空")
                        st.rerun()

            for i, item in enumerate(history):
                with st.expander(f"📌 {item['analysis_date']} ({item['model_name']})", expanded=(i==0)):
                    st.markdown(item['report'])
                    if st.button("❌ 删除此条记录", key=f"del_{code}_{i}"):
                        if stock_service.analysis_cache.delete_ai_analysis(ts_code, item['analysis_date']):
                            st.success("记录已删除")
                            st.rerun()
                    st.divider()
        else:
            st.info("暂无历史分析记录")

        st.markdown("---")
        
        # 2. 开启新研讨
        if st.button("🚀 智能诊断", key=f"ai_btn_{code}", type="primary"):
            # 使用空占位符，确保消息能实时显示
            log_placeholder = st.empty()
            
            with st.status("🧠 投研研讨会正在进行...", expanded=True) as status:
                st.write("🕵️ 数据管理员正在调取全息素材...")
                
                # 为了支持实时刷新，我们维护一个消息列表
                if 'ai_logs' not in st.session_state:
                    st.session_state.ai_logs = []
                st.session_state.ai_logs = [] # 每次开始清空

                def on_msg_received(sender, content):
                    import time
                    is_analyst = "Analyst" in sender
                    is_risk = "Risk" in sender
                    icon = "👨‍🏫" if is_analyst else ("⚖️" if is_risk else "🕵️")
                    
                    # 将消息存入 session_state 并显示
                    msg = {"sender": sender, "content": content, "icon": icon}
                    st.session_state.ai_logs.append(msg)
                    
                    # 在 status 内部渲染当前所有日志
                    with status:
                        with st.chat_message("assistant" if (is_analyst or is_risk) else "user", avatar=icon):
                            st.markdown(f"**{sender}**: {content}")
                    
                    # 给 UI 刷新留点时间
                    time.sleep(0.3) # 稍微加长一点，让用户能看清

                analyst = StockAnalystAgent(stock_service)
                try:
                    report = analyst.analyze_stock(code, message_callback=on_msg_received)
                    
                    # 保存分析结果
                    model_name = LLM_CONFIG['config_list'][0].get('model', 'Unknown')
                    stock_service.analysis_cache.save_ai_analysis(ts_code, report, model_name)
                    
                    status.update(label="✅ 研讨圆满结束！报告已存档。", state="complete", expanded=False)
                    
                    st.markdown("---")
                    st.markdown(f"### 📋 {name} ({code}) 最终智能诊断结论")
                    st.markdown(report)
                    st.balloons()
                except Exception as e:
                    status.update(label="❌ 研讨会异常中断", state="error")
                    st.error(f"AI 分析失败: {str(e)}")
