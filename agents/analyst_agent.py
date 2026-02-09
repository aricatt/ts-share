import autogen
from typing import Dict, Any
from agents.config import LLM_CONFIG

class StockAnalystAgent:
    """
    股票分析专家团队（三人研讨会模式）
    """
    def __init__(self, stock_service):
        self.stock_service = stock_service
        
        # 1. 定义资深分析师 (机会发现者)
        self.analyst = autogen.AssistantAgent(
            name="Senior_Analyst",
            system_message="""你是一位资深多头分析师。
你的任务是：
1. 从量价关系、资金流向、利好新闻中发现上涨逻辑。
2. 评估该股的基本面优势和成长性。
你倾向于发现股票的价值和机会点。每次发言请保持专业。""",
            llm_config=LLM_CONFIG,
        )

        # 2. 定义风险控制官 (风险挖掘者)
        self.risk_manager = autogen.AssistantAgent(
            name="Risk_Controller",
            system_message="""你是一位严厉的风险风控专家。
你的任务是：
1. 专门质疑分析师的乐观结论。
2. 深度从财务数据中找地雷（如营收虚增、债台高筑、现金流差）。
3. 解析新闻中的利空信息或潜在隐患。
你的目标是确保投资者不会因为盲目乐观而造成重大亏损。如果你认为风险过大，必须直言。""",
            llm_config=LLM_CONFIG,
        )

        # 3. 定义管理员 (主持人/数据源)
        self.user_proxy = autogen.UserProxyAgent(
            name="Admin",
            human_input_mode="NEVER",
            is_termination_msg=lambda x: x.get("content", "").find("TERMINATE") >= 0,
            code_execution_config=False,
        )

    def analyze_stock(self, code: str, message_callback=None) -> str:
        """
        启动三人投研研讨会
        """
        # 1. 获取数据素材
        data = self.stock_service.get_ai_analysis_data(code)
        
        # 2. 构造初始简报
        briefing = f"""各位，现在开始对 {data['stock_name']} ({data['ts_code']}) 进行研讨。
日期：{data['date']}

【全息素材】：
1. 近10日走势：{data['recent_prices']}
2. 财务大盘：{data['key_financials']}
3. 资金面：{data['money_flow']}
4. 消息面：{data['latest_news']}

请 Senior_Analyst 先发表意见，Risk_Controller 随后补充风险点。
最后由 Senior_Analyst 综合双方意见给出最终总结结论，并以 TERMINATE 结束。"""

        # 3. 创建群聊
        groupchat = autogen.GroupChat(
            agents=[self.user_proxy, self.analyst, self.risk_manager], 
            messages=[], 
            max_round=6,
            speaker_selection_method="auto"
        )
        
        manager = autogen.GroupChatManager(groupchat=groupchat, llm_config=LLM_CONFIG)

        # 4. 注册实时回调
        if message_callback:
            def print_messages(recipient, messages, sender, config):
                last_msg = messages[-1]
                content = last_msg.get("content", "")
                if content and "TERMINATE" not in content:
                    message_callback(sender.name, content)
                return False, None
            
            # 对所有核心 Agent 和主持人(Manager)都注册，确保 100% 捕获
            for agent in [self.analyst, self.risk_manager, self.user_proxy, manager]:
                agent.register_reply([autogen.Agent, None], reply_func=print_messages, position=0)

        # 5. 启动会话
        self.user_proxy.initiate_chat(
            manager,
            message=briefing
        )
        
        # 返回汇总报告
        return groupchat.messages[-1]["content"].replace("TERMINATE", "")
