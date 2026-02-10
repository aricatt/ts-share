import autogen
from typing import Dict, Any
import time
from agents.config import LLM_CONFIG

class StockAnalystAgent:
    """
    股票分析专家团队（三人研讨会模式）
    """
    def __init__(self, stock_service):
        self.stock_service = stock_service
        
        # 1. 定义资深分析师
        self.analyst = autogen.AssistantAgent(
            name="Senior_Analyst",
            system_message="""你是一位资深多头分析师。
你的任务是发现股票的上涨逻辑。每次发言请保持专业。汇总阶段请结合风控意见。""",
            llm_config=LLM_CONFIG,
        )

        # 2. 定义风险控制官
        self.risk_manager = autogen.AssistantAgent(
            name="Risk_Controller",
            system_message="""你是一位严厉的风控专家。
你的任务是专门质疑分析师的乐观结论，挖掘地雷和隐患。""",
            llm_config=LLM_CONFIG,
        )

        # 3. 定义管理员
        self.user_proxy = autogen.UserProxyAgent(
            name="Admin",
            human_input_mode="NEVER",
            is_termination_msg=lambda x: x.get("content", "").find("TERMINATE") >= 0,
            code_execution_config=False,
        )

    def analyze_stock(self, code: str, message_callback=None) -> str:
        """
        核心分析函数
        """
        # --- 阶段一：调取数据 ---
        if message_callback:
            message_callback("系统", "正在同步市场数据与财务报表...")
        
        data = self.stock_service.get_ai_analysis_data(code)
        
        if message_callback:
            message_callback("系统", f"数据同步完成：已获取 {data['stock_name']} 全息素材。")
            time.sleep(0.3)
        
        # --- 阶段二：组建专家团 ---
        briefing = f"""各位，现在开始对 {data['stock_name']} ({data['ts_code']}) 进行研讨。
日期：{data['date']}

【全息素材】：
1. 近10日走势：{data['recent_prices']}
2. 财务大盘：{data['key_financials']}
3. 资金面：{data['money_flow']}
4. 消息面：{data['latest_news']}

请 Senior_Analyst 先发表意见，Risk_Controller 随后补充风险点。
最后由 Senior_Analyst 综合双方意见给出最终总结结论，并以 TERMINATE 结束。"""

        # 这里显式回调一次 Admin 的话，让用户看到发了什么
        if message_callback:
            message_callback("Admin", "正在分发全息素材给专家团...")
            message_callback("Admin", briefing)

        groupchat = autogen.GroupChat(
            agents=[self.user_proxy, self.analyst, self.risk_manager], 
            messages=[], 
            max_round=8,
            speaker_selection_method="auto"
        )
        manager = autogen.GroupChatManager(groupchat=groupchat, llm_config=LLM_CONFIG)

        # 注册钩子
        if message_callback:
            def print_messages(recipient, messages, sender, config):
                last_msg = messages[-1]
                content = last_msg.get("content", "")
                if content and "TERMINATE" not in content:
                    # 确保传出正确的发送者名称
                    s_name = sender.name if hasattr(sender, "name") else str(sender)
                    message_callback(s_name, content)
                return False, None
            
            for agent in [self.analyst, self.risk_manager, self.user_proxy, manager]:
                agent.register_reply([autogen.Agent, None], reply_func=print_messages, position=0)

        # --- 阶段三：启动云端研讨 ---
        if message_callback:
            message_callback("系统", "研讨会正在公网加密隧道进行，等待 LLM 首次响应 (qwen-max)...")

        try:
            self.user_proxy.initiate_chat(manager, message=briefing, clear_history=True)
        except Exception as e:
            if message_callback:
                message_callback("系统", f"⚠️ 会话异常: {str(e)}")
            raise e
        
        if message_callback:
            message_callback("系统", "研讨圆满结束，报告输出中...")
            
        return groupchat.messages[-1]["content"].replace("TERMINATE", "") if groupchat.messages else "未能生成报告"
