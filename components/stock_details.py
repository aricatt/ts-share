import streamlit as st
import pandas as pd
from datetime import datetime
from services import StockService
from agents.analyst_agent import StockAnalystAgent
from components.charts import render_chart, create_kline_chart
from agents.config import LLM_CONFIG

@st.dialog("股票详情诊断", width="large")
def show_stock_details(code: str, name: str, stock_service: StockService, rule_name: str = None):
    """
    股票详情弹窗组件
    """
    ts_code = stock_service._to_ts_code(code)
    
    # 标题栏
    col1, col2 = st.columns([3, 1])
    with col1:
        st.subheader(f"🔍 {name} ({ts_code})")
    # 获取当前价格（用于收藏记录）
    current_price = None
    df_hist = stock_service.get_history(code, days=5)
    if df_hist is not None and not df_hist.empty:
        current_price = float(df_hist.iloc[-1]['收盘'])

    with col2:
        if rule_name:
            is_collected = stock_service.is_collected(code, rule_name)
            btn_label = "⭐ 取消收藏" if is_collected else "➕ 加入收藏"
            if st.button(btn_label, use_container_width=True, type="primary" if not is_collected else "secondary"):
                if is_collected:
                    if stock_service.remove_collected_stock(code, rule_name):
                        st.toast(f"已从【{rule_name}】中移除")
                        st.rerun()
                else:
                    if stock_service.collect_stock(code, name, rule_name, price=current_price):
                        st.toast(f"已保存到【{rule_name}】收藏")
                        st.rerun()

    # --- 标签页布局 ---
    tab1, tab_profile, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📈 K线走势", "🏢 公司概况", "📊 财务指标", "💰 资金流向", "📢 重要公告", "🤖 AI 智能诊断", "🗨️ 追问分析师"
    ])

    with tab1:
        df_hist = stock_service.get_history(code, days=250)
        if df_hist is not None and not df_hist.empty:
            chart = create_kline_chart(df_hist, title=f"{name} 历史K线")
            render_chart(chart, height=500)
        else:
            st.warning("暂无历史 K 线数据")

    with tab_profile:
        st.markdown("#### 🏢 上市公司基本信息")
        with st.spinner("正在获取公司详情..."):
            df_company = stock_service.get_company_info(ts_code)
        if df_company is not None and not df_company.empty:
            info = df_company.iloc[0]
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("董事长", info.get('chairman', 'N/A'))
            c2.metric("总经理", info.get('manager', 'N/A'))
            c3.metric("注册资本", f"{info.get('reg_capital', 0)/10000:.2f} 亿")
            c4.metric("成立日期", info.get('setup_date', 'N/A'))
            st.markdown("---")
            st.markdown(f"**📍 所在地**：{info.get('province', '')} - {info.get('city', '')}")
            st.markdown("**📖 公司简介**")
            st.info(info.get('introduction', '暂无简介'))
            st.markdown("**🛠️ 主营业务**")
            st.success(info.get('main_business', '暂无业务描述'))
            with st.expander("🔍 查看经营范围"):
                st.write(info.get('business_scope', '暂无描述'))

            # --- 行业地位模块 ---
            st.markdown("---")
            st.markdown("#### 🏆 行业地位与排名")
            rank_info = stock_service.get_industry_ranking(ts_code)
            if rank_info:
                r1, r2, r3 = st.columns(3)
                r1.metric("所属行业", rank_info['industry'])
                r2.metric("市值排名", f"{rank_info['rank_market_cap']} / {rank_info['total_count']}")
                r3.metric("涨幅排名", f"{rank_info['rank_pct_chg']} / {rank_info['total_count']}")
                
                # 提示
                if rank_info['rank_market_cap'] <= 3:
                    st.success(f"✨ 该标的是 **{rank_info['industry']}** 行业的领军龙头（市值前三）！")
                elif rank_info['rank_market_cap'] <= 10:
                    st.info(f"🚀 该标的是 **{rank_info['industry']}** 行业的骨干力量，市值排名在前十。")
                
                # 行业领头羊展示
                st.markdown("**行业总市值前三名（真龙头）：**")
                leader_cols = st.columns(3)
                for i, leader in enumerate(rank_info['leaders']):
                    with leader_cols[i]:
                        st.markdown(f"**{i+1}. {leader['名称']}**")
                        st.caption(f"市值: {leader['总市值']/10000:.2f} 亿")
            else:
                st.info("💡 暂无行业排名数据，请先同步历史行情记录。")
        else:
            st.info("💡 暂无该公司简介信息")

    with tab2:
        st.markdown("#### 📊 核心财务指标")
        fina = stock_service.get_fundamental(ts_code, 'fina_indicator')
        if fina is not None and not fina.empty:
            fina_map = {'end_date': '报告期', 'eps': '每股收益', 'roe': '净资产收益率(%)', 'netprofit_margin': '销售净利率(%)', 'grossprofit_margin': '销售毛利率(%)', 'debt_to_assets': '资产负债率(%)', 'netprofit_yoy': '净利润增长(%)', 'tr_yoy': '营收增长(%)', 'bps': '每股净资产'}
            latest_fina = fina.iloc[0]
            col_f1, col_f2, col_f3 = st.columns(3)
            col_f1.metric("销售净利率", f"{latest_fina.get('netprofit_margin', 0):.2f}%")
            col_f2.metric("净资产收益率", f"{latest_fina.get('roe', 0):.2f}%")
            col_f3.metric("资产负债率", f"{latest_fina.get('debt_to_assets', 0):.2f}%")
            st.dataframe(fina[fina.columns.intersection(fina_map.keys())].rename(columns=fina_map).head(10), use_container_width=True, hide_index=True)
        else:
            st.info("暂无历史财务数据")

    with tab3:
        st.markdown("#### 💰 资金流向 (单位: 万元)")
        money = stock_service.get_money_flow_cached(ts_code, datetime.now().strftime("%Y%m%d"))
        if money is not None and not money.empty:
            money_map = {'trade_date': '交易日期', 'buy_sm_amount': '小单买入', 'sell_sm_amount': '小单卖出', 'buy_md_amount': '中单买入', 'sell_md_amount': '中单卖出', 'buy_lg_amount': '大单买入', 'sell_lg_amount': '大单卖出', 'buy_elg_amount': '特大单买入', 'sell_elg_amount': '特大单卖出', 'net_mf_amount': '净流入额'}
            st.dataframe(money[money.columns.intersection(money_map.keys())].rename(columns=money_map).sort_values('交易日期', ascending=False), use_container_width=True, hide_index=True)
        else:
            st.info("近期暂无资金流向数据")

    with tab4:
        st.markdown("#### 📢 重要公告与新闻")
        news = stock_service.get_stock_news(ts_code)
        if news is not None and not news.empty:
            for _, item in news.iterrows():
                with st.container():
                    st.caption(item['ann_date'])
                    st.markdown(f"**{item['title']}**")
                    if 'url' in item and item['url']: st.markdown(f"[🔗 查看详情]({item['url']})")
                    st.divider()
        else:
            st.info("最近暂无重要公告")

    with tab5:
        st.markdown("#### 🤖 AutoGen 三人投研专家团")
        st.info("AI 专家团将通过多轮博弈进行深度诊断。")
        history = stock_service.analysis_cache.get_ai_analysis_history(ts_code)
        if history:
            with st.expander("📝 历史分析记录", expanded=False):
                for i, row in enumerate(history):
                    st.markdown(f"**{row['analysis_date']} ({row['model_name']})**")
                    st.markdown(row['report'])
                    st.button("❌ 删除", key=f"del_{code}_{i}", on_click=lambda r=row: stock_service.analysis_cache.delete_ai_analysis(ts_code, r['analysis_date']))
                    st.divider()
        st.markdown("---")
        if st.button("🚀 开始智能诊断", key=f"ai_btn_{code}", type="primary"):
            log_area = st.container()
            with st.status("🧠 投研研讨会火热进行中...", expanded=True) as status:
                def on_msg_received(sender, content):
                    import time
                    icon = "⚙️" if "系统" in sender else ("👨‍🏫" if "Analyst" in sender else ("⚖️" if "Risk" in sender else "�️"))
                    with log_area:
                        with st.chat_message("assistant" if ("Analyst" in sender or "Risk" in sender) else "user", avatar=icon):
                            st.markdown(f"**{sender}**: {content}")
                    time.sleep(0.5)
                try:
                    report = StockAnalystAgent(stock_service).analyze_stock(code, message_callback=on_msg_received)
                    stock_service.analysis_cache.save_ai_analysis(ts_code, report, LLM_CONFIG['config_list'][0].get('model', 'Unknown'))
                    status.update(label="✅ 分析会议圆满完成！", state="complete", expanded=False)
                    st.markdown(report)
                    st.balloons()
                except Exception as e:
                    status.update(label="❌ 诊断中断", state="error")
                    st.error(f"分析失败: {str(e)}")

    with tab6:
        st.markdown("#### 🗨️ 与资深分析师实时对话")
        chat_key = f"chat_history_{code}"
        if chat_key not in st.session_state: st.session_state[chat_key] = []
        chat_container = st.container(height=450)
        with chat_container:
            if not st.session_state[chat_key]: st.info(f"关于 {name} ({code})，您想了解什么？")
            for msg in st.session_state[chat_key]:
                with st.chat_message(msg["role"], avatar="👨‍🏫" if msg["role"] == "assistant" else "👤"): st.markdown(msg["content"])
        
        if prompt := st.chat_input(f"询问关于 {name} 的问题...", key=f"chat_input_{code}"):
            with chat_container:
                with st.chat_message("user", avatar="👤"): st.markdown(prompt)
            st.session_state[chat_key].append({"role": "user", "content": prompt})
            
            with chat_container:
                # 使用 status 组件实时展示分析师的内部动作
                with st.status("👨‍🏫 分析师正在思考...", expanded=True) as status:
                    thought_container = st.empty()
                    def on_chat_msg(sender, content):
                        if "系统" in sender:
                            status.write(f"⚙️ {content}")
                        else:
                            # 如果是分析师在组织语言，显示在状态栏
                            status.write(f"✍️ {sender} 正在组织回答...")
                    
                    try:
                        analyst_agent = StockAnalystAgent(stock_service)
                        response = analyst_agent.ask_analyst(code, prompt, st.session_state[chat_key], message_callback=on_chat_msg)
                        status.update(label="✅ 思考完成", state="complete", expanded=False)
                        
                        # 在对话流中展示最终回答
                        with st.chat_message("assistant", avatar="👨‍🏫"):
                            st.markdown(response)
                        st.session_state[chat_key].append({"role": "assistant", "content": response})
                        st.rerun()
                    except Exception as e:
                        status.update(label="❌ 对话中断", state="error")
                        st.error(f"对话异常: {str(e)}")
