import autogen
from typing import Dict, Any, List
import time
from agents.config import LLM_CONFIG

class StockAnalystAgent:
    """
    股票分析专家团队
    """
    def __init__(self, stock_service):
        self.stock_service = stock_service
        
        # 基础分析师配置
        self.analyst_config = {
            "name": "Senior_Analyst",
            "system_message": "你是一位资深多头分析师，擅长发现上涨逻辑。请专业、客观地回答。",
            "llm_config": LLM_CONFIG,
        }

        # 基础风控配置
        self.risk_config = {
            "name": "Risk_Controller",
            "system_message": "你是一位严厉的风控专家，擅长发现隐患。",
            "llm_config": LLM_CONFIG,
        }

    def analyze_stock(self, code: str, message_callback=None) -> str:
        """
        启动研讨会 (多回合博弈模式)
        """
        if message_callback: message_callback("系统", "正在同步市场全息数据...")
        data = self.stock_service.get_ai_analysis_data(code)
        
        briefing = f"""各位，开始对 {data['stock_name']} ({data['ts_code']}) 研讨。
【数据汇总】：行情 {data['recent_prices'][-5:]}, 财务 {data['key_financials']}, 新闻 {data['latest_news'][:5]}
请 Senior_Analyst 先发表意见，Risk_Controller 补充，最后 Senior_Analyst 总结并以 TERMINATE 结束。"""

        # 创建临时会话 Agent
        analyst = autogen.AssistantAgent(**self.analyst_config)
        risk = autogen.AssistantAgent(**self.risk_config)
        admin = autogen.UserProxyAgent(name="Admin", human_input_mode="NEVER", is_termination_msg=lambda x: "TERMINATE" in x.get("content", ""), code_execution_config=False)
        
        # 注册回调
        groupchat = autogen.GroupChat(agents=[admin, analyst, risk], messages=[], max_round=8, speaker_selection_method="auto")
        manager = autogen.GroupChatManager(groupchat=groupchat, llm_config=LLM_CONFIG)

        if message_callback:
            def _cb(recipient, messages, sender, config):
                content = messages[-1].get("content", "")
                if content and "TERMINATE" not in content:
                    message_callback(sender.name if hasattr(sender, "name") else str(sender), content)
                return False, None
            for a in [analyst, risk, admin, manager]: a.register_reply([autogen.Agent, None], reply_func=_cb, position=0)

        if message_callback: message_callback("Admin", "素材已分发，等待专家席响应...")
        admin.initiate_chat(manager, message=briefing, clear_history=True)
        return groupchat.messages[-1]["content"].replace("TERMINATE", "") if groupchat.messages else "分析失败"

    def ask_analyst(self, code: str, user_question: str, chat_history: List[Dict] = None, message_callback=None) -> str:
        """
        1对1 提问 (单回合极速响应模式)
        """
        # 1. 精简数据，防止 Token 过载导致卡死
        if message_callback: message_callback("系统", "正在提取个股关键特征数据...")
        data = self.stock_service.get_ai_analysis_data(code)
        
        # 2. 构造干净的上下文
        # 仅取最近1天行情和最新的财报记录
        context = f"""
你是资深分析师。当前个股：{data['stock_name']} ({data['ts_code']})。
参考数据：价格 {data['recent_prices'][-1:]}, 财务 {data['key_financials']}, 行业新闻 {data['latest_news'][:3]}。
用户追问：{user_question}
请直接给出回答，无需客套。
"""
        # 3. 创建纯净的临时 Agent
        analyst = autogen.AssistantAgent(name="Senior_Analyst", system_message="你是资深分析师，请专业回答。", llm_config=LLM_CONFIG)
        user_proxy = autogen.UserProxyAgent(name="User", human_input_mode="NEVER", max_consecutive_auto_reply=0, code_execution_config=False) # 强制 0 次自动回复，即收到就停

        # 4. 注册实时回调
        if message_callback:
            def _chat_cb(recipient, messages, sender, config):
                content = messages[-1].get("content", "")
                if content and sender.name != "User":
                    message_callback(sender.name, content)
                return False, None
            analyst.register_reply([autogen.Agent, None], reply_func=_chat_cb, position=0)

        # 5. 发起对话
        if message_callback: message_callback("系统", "信号已发出，正在获取分析师意见...")
        
        try:
            # 使用简单的 initiate_chat，并设置明确的交互轮数
            res = user_proxy.initiate_chat(
                analyst,
                message=context,
                summary_method="last_msg",
                clear_history=True
            )
            return res.summary if hasattr(res, 'summary') else str(res)
        except Exception as e:
            error_msg = f"连接超时或服务波动: {str(e)}"
            if message_callback: message_callback("系统", f"⚠️ {error_msg}")
            return error_msg
