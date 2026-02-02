"""
选股策略基类
所有策略都应继承此基类
"""
from abc import ABC, abstractmethod
import pandas as pd
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field


@dataclass
class StepResult:
    """单步执行结果"""
    step_name: str           # 步骤名称
    before_count: int        # 执行前数量
    after_count: int         # 执行后数量
    filtered_count: int      # 被过滤数量
    description: str = ""    # 步骤描述


class StepTracker:
    """步骤跟踪器"""
    
    def __init__(self):
        self.steps: List[StepResult] = []
        self._current_count: int = 0
    
    def start(self, df: pd.DataFrame):
        """开始跟踪"""
        self._current_count = len(df)
        self.steps = []
    
    def record(self, step_name: str, df: pd.DataFrame, description: str = ""):
        """
        记录一个步骤
        
        Args:
            step_name: 步骤名称
            df: 当前 DataFrame
            description: 步骤描述
        """
        after_count = len(df)
        step = StepResult(
            step_name=step_name,
            before_count=self._current_count,
            after_count=after_count,
            filtered_count=self._current_count - after_count,
            description=description,
        )
        self.steps.append(step)
        self._current_count = after_count
    
    def get_summary(self) -> str:
        """获取执行摘要"""
        lines = ["📋 执行步骤跟踪：", ""]
        for i, step in enumerate(self.steps, 1):
            emoji = "✅" if step.filtered_count == 0 else "🔻"
            lines.append(
                f"{i}. {emoji} {step.step_name}: "
                f"{step.before_count} → {step.after_count} "
                f"(过滤 {step.filtered_count} 只)"
            )
            if step.description:
                lines.append(f"   └─ {step.description}")
        return "\n".join(lines)
    
    def to_dataframe(self) -> pd.DataFrame:
        """转换为 DataFrame 用于展示"""
        return pd.DataFrame([
            {
                "步骤": s.step_name,
                "执行前": s.before_count,
                "执行后": s.after_count,
                "过滤数": s.filtered_count,
                "说明": s.description,
            }
            for s in self.steps
        ])


class BaseRule(ABC):
    """选股策略基类"""
    
    # 策略名称
    name: str = "未命名策略"
    
    # 策略描述
    description: str = ""
    
    # 数据源类型：'zt_pool' (涨停股池) | 'all_stocks' (全A股)
    data_source: str = "zt_pool"
    
    # 是否需要历史K线数据
    requires_history: bool = False
    
    # 历史数据天数（如果需要）
    history_days: int = 120
    
    def __init__(self):
        self.tracker = StepTracker()
    
    @abstractmethod
    def apply(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        应用策略筛选
        
        Args:
            df: 股票数据 DataFrame
            **kwargs: 额外参数（如历史数据获取器）
        
        Returns:
            筛选后的 DataFrame
        """
        pass
    
    def get_tracker(self) -> StepTracker:
        """获取步骤跟踪器"""
        return self.tracker
    
    def get_params(self) -> Dict[str, Any]:
        """
        获取策略参数（用于 UI 展示）
        
        Returns:
            参数字典
        """
        return {}
    
    def __str__(self) -> str:
        return f"{self.name}: {self.description}"
