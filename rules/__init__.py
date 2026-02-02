"""
选股策略模块
"""
from .base import BaseRule, StepTracker, StepResult
from .rule_small_cap_limit_up import RuleSmallCapLimitUp
from .rule_dragon_pullback import RuleDragonPullback

# 策略注册表
RULE_REGISTRY = {
    "小盘涨停异动": RuleSmallCapLimitUp,
    "龙回头": RuleDragonPullback,
}

def get_rule(name: str) -> BaseRule:
    """获取策略实例"""
    if name not in RULE_REGISTRY:
        raise ValueError(f"未知策略: {name}")
    return RULE_REGISTRY[name]()

def get_all_rules() -> list:
    """获取所有策略名称"""
    return list(RULE_REGISTRY.keys())

__all__ = [
    "BaseRule",
    "StepTracker",
    "StepResult",
    "RuleSmallCapLimitUp",
    "RuleDragonPullback",
    "get_rule",
    "get_all_rules",
    "RULE_REGISTRY",
]
