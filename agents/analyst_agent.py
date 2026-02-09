import autogen
from typing import Dict, Any
from agents.config import LLM_CONFIG

class StockAnalystAgent:
    """
    股票分析专家智能体
    """
    def __init__(self, stock_service):
        self.stock_service = stock_service
        
        # 1. 定义分析师（Assistant）
        self.analyst = autogen.AssistantAgent(
            name="Senior_Stock_Analyst",
            system_message="""你是一位拥有 20 年经验的资深 A 股证券分析师。
你的任务是根据提供的数据（量价、财务、资金、新闻）对个股进行全方位诊断。
你的分析必须包含：
1. 趋势分析：基于最近价格和均线（如有）判断走势。
2. 财务评估：核心指标是否健康，基本面是否有隐忧。
3. 资金博弈：近期主力资金是大规模流入还是流出。
4. 消息解读：解析最新新闻对股价的潜在利好或利空影响。
5. 综合结论：给出结论，指出该股的风险点和机会点。
请用专业、客观、严谨的中文进行回复。""",
            llm_config=LLM_CONFIG,
        )

        # 2. 定义代理（UserProxy）- 负责获取数据
        self.user_proxy = autogen.UserProxyAgent(
            name="Admin",
            human_input_mode="NEVER",
            max_consecutive_auto_reply=2,
            is_termination_msg=lambda x: x.get("content", "").find("TERMINATE") >= 0,
            code_execution_config=False, # 我们不需要它运行代码，只需要调工具
        )

    def analyze_stock(self, code: str) -> str:
        """
        开始对某只股票进行智能分析
        """
        # 获取底层数据
        data = self.stock_service.get_ai_analysis_data(code)
        
        # 构造初始提问
        prompt = f"""请对以下股票进行深度分析决策报告：
股票名称：{data['stock_name']} ({data['ts_code']})
当前分析日期：{data['date']}

数据概览：
---
1. 最近价格走势（前10日）：
{data['recent_prices']}

2. 核心财务指标：
{data['key_financials']}

3. 资金流向：
{data['money_flow']}

4. 最新头条新闻：
{data['latest_news']}
---
请根据以上数据给出一份专业的诊断报告。最后请以 'TERMINATE' 结尾。"""

        # 启动会话
        self.user_proxy.initiate_chat(
            self.analyst,
            message=prompt,
            clear_history=True
        )
        
        # 返回分析师的最后一条消息
        return self.user_proxy.last_message()["content"].replace("TERMINATE", "")
