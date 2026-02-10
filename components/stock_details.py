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
                    if stock_service.collect_stock(code, name, rule_name):
                        st.toast(f"已保存到【{rule_name}】收藏")
                        st.rerun()

    # --- 标签页布局 ---
    tab1, tab_profile, tab2, tab3, tab4, tab5 = st.tabs([
        "📈 K线走势", 
        "🏢 公司概况",
        "📊 财务指标", 
        "💰 资金流向", 
        "📢 重要公告", 
        "🤖 AI 智能诊断"
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
            
            # 第一排：核心管理层与资本
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("董事长", info.get('chairman', 'N/A'))
            c2.metric("总经理", info.get('manager', 'N/A'))
            c3.metric("注册资本", f"{info.get('reg_capital', 0)/10000:.2f} 亿")
            c4.metric("成立日期", info.get('setup_date', 'N/A'))
            
            st.markdown("---")
            
            # 第二排：省份城市
            st.markdown(f"**📍 所在地**：{info.get('province', '')} - {info.get('city', '')}")
            
            # 企业简介
            st.markdown("**📖 公司简介**")
            st.info(info.get('introduction', '暂无简介'))
            
            # 主营业务
            st.markdown("**🛠️ 主营业务**")
            st.success(info.get('main_business', '暂无业务描述'))
            
            # 经营范围
            with st.expander("🔍 查看经营范围"):
                st.write(info.get('business_scope', '暂无描述'))
        else:
            st.info("💡 暂无该公司简介信息或 Tushare 积分不足")

    with tab2:
        st.markdown("#### 📊 核心财务指标 (历史滚动)")
        fina = stock_service.get_fundamental(ts_code, 'fina_indicator')
        if fina is not None and not fina.empty:
            # 字段映射表（通俗中文）
            fina_map = {
                'end_date': '报告期',
                'eps': '每股收益',
                'roe': '净资产收益率(%)',
                'netprofit_margin': '销售净利率(%)',
                'grossprofit_margin': '销售毛利率(%)',
                'debt_to_assets': '资产负债率(%)',
                'netprofit_yoy': '净利润增长(%)',
                'tr_yoy': '营收增长(%)',
                'bps': '每股净资产',
                'current_ratio': '流动比率',
                'quick_ratio': '速动比率'
            }
            
            latest_fina = fina.iloc[0]
            col_f1, col_f2, col_f3 = st.columns(3)
            col_f1.metric("销售净利率", f"{latest_fina.get('netprofit_margin', 0):.2f}%")
            col_f2.metric("净资产收益率 (ROE)", f"{latest_fina.get('roe', 0):.2f}%")
            col_f3.metric("资产负债率", f"{latest_fina.get('debt_to_assets', 0):.2f}%")
            
            # 处理展示表格
            disp_fina = fina[fina.columns.intersection(fina_map.keys())].rename(columns=fina_map)
            st.dataframe(disp_fina.head(10), use_container_width=True, hide_index=True)
        else:
            st.info("暂无历史财务数据")

    with tab3:
        st.markdown("#### 💰 资金流向 (单位: 万元)")
        last_date = datetime.now().strftime("%Y%m%d")
        money = stock_service.get_money_flow_cached(ts_code, last_date)
        if money is not None and not money.empty:
            # 字段映射表（通俗中文）
            money_map = {
                'trade_date': '交易日期',
                'buy_sm_amount': '小单买入',
                'sell_sm_amount': '小单卖出',
                'buy_md_amount': '中单买入',
                'sell_md_amount': '中单卖出',
                'buy_lg_amount': '大单买入',
                'sell_lg_amount': '大单卖出',
                'buy_elg_amount': '特大单买入',
                'sell_elg_amount': '特大单卖出',
                'net_mf_amount': '净流入额'
            }
            # 处理展示表格
            disp_money = money[money.columns.intersection(money_map.keys())].rename(columns=money_map)
            st.dataframe(disp_money.sort_values('交易日期', ascending=False), use_container_width=True, hide_index=True)
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
                    if 'url' in item and item['url']:
                        st.markdown(f"[🔗 查看详情]({item['url']})")
                    st.divider()
        else:
            st.info("最近暂无重要公告")

    with tab5:
        st.markdown("#### 🤖 AutoGen 三人投研专家团")
        st.info("AI 专家团将通过多轮博弈进行深度诊断。")
        
        # 1. 历史记录
        history = stock_service.analysis_cache.get_ai_analysis_history(ts_code)
        if history:
            with st.expander("📝 历史分析记录", expanded=False):
                for i, row in enumerate(history):
                    st.markdown(f"**{row['analysis_date']} ({row['model_name']})**")
                    st.markdown(row['report'])
                    st.button("❌ 删除", key=f"del_{code}_{i}", on_click=lambda r=row: stock_service.analysis_cache.delete_ai_analysis(ts_code, r['analysis_date']))
                    st.divider()

        st.markdown("---")
        
        # 2. 诊断逻辑
        if st.button("🚀 开始智能诊断", key=f"ai_btn_{code}", type="primary"):
            log_area = st.container() # 预留主日志区
            with st.status("🧠 投研研讨会火热进行中...", expanded=True) as status:
                
                # 定义回调
                def on_msg_received(sender, content):
                    import time
                    # 确定头像图标
                    if "系统" in sender: icon = "⚙️"
                    elif "Analyst" in sender: icon = "👨‍🏫"
                    elif "Risk" in sender: icon = "⚖️"
                    elif "Admin" in sender: icon = "🕵️"
                    else: icon = "👤"
                    
                    # 实时输出消息到预留区
                    with log_area:
                        with st.chat_message("assistant" if ("Analyst" in sender or "Risk" in sender) else "user", avatar=icon):
                            st.markdown(f"**{sender}**: {content}")
                    
                    # 强制呼吸延时，利于 Streamlit 异步刷新
                    time.sleep(0.5)

                analyst_agent = StockAnalystAgent(stock_service)
                try:
                    # 开始分析
                    report = analyst_agent.analyze_stock(code, message_callback=on_msg_received)
                    
                    # 保存结果
                    model = LLM_CONFIG['config_list'][0].get('model', 'Unknown')
                    stock_service.analysis_cache.save_ai_analysis(ts_code, report, model)
                    
                    status.update(label="✅ 分析会议圆满完成！", state="complete", expanded=False)
                    st.markdown("### 📋 深度诊断最终结论")
                    st.markdown(report)
                    st.balloons()
                except Exception as e:
                    status.update(label="❌ 诊断中断", state="error")
                    st.error(f"分析失败: {str(e)}")
