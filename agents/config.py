"""
AutoGen 智能体配置文件
"""
import os

# 配置通义千问 (DashScope)
LLM_CONFIG = {
    "config_list": [
        {
            "model": "qwen-max", # 建议使用 qwen-max 以获得最强分析能力
            "api_key": "sk-6134bc2d9f5f4cb4b08ac624cfddb68a", # 你的千问 Key
            "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1", # 阿里云 DashScope 兼容接口
            "api_type": "openai",
        }
    ],
    "cache_seed": None, # 设为 None 禁用本地缓存，确保每次点击都是实时分析
    "temperature": 0.1, # 保持严谨，不乱猜
}
