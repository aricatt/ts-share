"""
分析数据缓存服务
用于存储基本面、资金面等分析维度的数据缓存
使用独立的 SQLite 数据库，与历史交易数据分离
"""
import os
import json
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from enum import Enum


class CacheType(Enum):
    """缓存类型枚举"""
    FUNDAMENTAL = "fundamental"      # 基本面数据（财务指标、利润表等）
    MONEY_FLOW = "money_flow"        # 资金流向
    DAILY_BASIC = "daily_basic"      # 每日基础指标（PE/PB等）
    NEWS = "news"                    # 消息面（短期缓存）


class AnalysisCacheService:
    """
    分析数据缓存服务
    
    特点：
    - 使用独立的 SQLite 数据库（analysis_cache.db）
    - 按需缓存，支持 TTL 过期机制
    - 不影响核心历史交易数据
    
    缓存策略：
    - 基本面数据：30 天过期（财报季更新）
    - 资金面数据：当日有效
    - 每日指标：当日有效
    - 消息面：1 小时过期（或不缓存）
    """
    
    # 默认 TTL 配置（单位：天）
    DEFAULT_TTL = {
        CacheType.FUNDAMENTAL: 30,      # 基本面 30 天
        CacheType.MONEY_FLOW: 1,        # 资金流 1 天
        CacheType.DAILY_BASIC: 1,       # 日指标 1 天
        CacheType.NEWS: 0.04,           # 消息面 ~1 小时
    }
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.db_path = os.path.join(data_dir, "analysis_cache.db")
        
        # 确保目录存在
        os.makedirs(data_dir, exist_ok=True)
        
        # 初始化数据库
        self._init_db()
    
    def _init_db(self):
        """初始化数据库表结构"""
        with sqlite3.connect(self.db_path) as conn:
            # 启用 WAL 模式提升并发性能
            conn.execute("PRAGMA journal_mode = WAL")
            # 缓存数据可容忍丢失，关闭同步提升写入速度
            conn.execute("PRAGMA synchronous = OFF")
            
            # 基本面缓存表
            conn.execute('''
                CREATE TABLE IF NOT EXISTS fundamental_cache (
                    ts_code TEXT NOT NULL,
                    data_type TEXT NOT NULL,
                    data_json TEXT NOT NULL,
                    fetched_at TIMESTAMP NOT NULL,
                    expires_at TIMESTAMP NOT NULL,
                    PRIMARY KEY (ts_code, data_type)
                )
            ''')
            
            # 资金流向缓存表
            conn.execute('''
                CREATE TABLE IF NOT EXISTS money_flow_cache (
                    ts_code TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    data_json TEXT NOT NULL,
                    fetched_at TIMESTAMP NOT NULL,
                    expires_at TIMESTAMP NOT NULL,
                    PRIMARY KEY (ts_code, trade_date)
                )
            ''')
            
            # 每日基础指标缓存表（PE/PB/换手率等）
            conn.execute('''
                CREATE TABLE IF NOT EXISTS daily_basic_cache (
                    ts_code TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    data_json TEXT NOT NULL,
                    fetched_at TIMESTAMP NOT NULL,
                    expires_at TIMESTAMP NOT NULL,
                    PRIMARY KEY (ts_code, trade_date)
                )
            ''')
            
            # 消息面缓存表（新闻、公告等）
            conn.execute('''
                CREATE TABLE IF NOT EXISTS news_cache (
                    ts_code TEXT NOT NULL,
                    query_hash TEXT NOT NULL,
                    data_json TEXT NOT NULL,
                    fetched_at TIMESTAMP NOT NULL,
                    expires_at TIMESTAMP NOT NULL,
                    PRIMARY KEY (ts_code, query_hash)
                )
            ''')

            # AI 分析历史记录表（同一只股票同一天只保留一条最新记录）
            conn.execute('''
                CREATE TABLE IF NOT EXISTS ai_analysis_history (
                    ts_code TEXT NOT NULL,
                    analysis_date TEXT NOT NULL,
                    report TEXT NOT NULL,
                    model_name TEXT,
                    created_at TIMESTAMP NOT NULL,
                    PRIMARY KEY (ts_code, analysis_date)
                )
            ''')
            
            # 创建索引
            conn.execute('CREATE INDEX IF NOT EXISTS idx_fundamental_expires ON fundamental_cache (expires_at)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_money_flow_expires ON money_flow_cache (expires_at)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_daily_basic_expires ON daily_basic_cache (expires_at)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_news_expires ON news_cache (expires_at)')
            
            conn.commit()

    # ==================== AI 分析历史记录 ====================

    def save_ai_analysis(self, ts_code: str, report: str, model_name: str = None) -> bool:
        """保存 AI 分析报告（覆盖当日旧记录）"""
        try:
            now = datetime.now()
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO ai_analysis_history (ts_code, analysis_date, report, model_name, created_at)
                    VALUES (?, ?, ?, ?, ?)
                ''', (ts_code, now.strftime("%Y-%m-%d"), report, model_name, now.isoformat()))
                conn.commit()
                return True
        except Exception as e:
            print(f"保存 AI 分析报告失败: {e}")
            return False

    def get_ai_analysis_history(self, ts_code: str, limit: int = 10) -> List[Dict[str, Any]]:
        """获取个股的 AI 分析历史"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute('''
                    SELECT report, model_name, analysis_date, created_at 
                    FROM ai_analysis_history 
                    WHERE ts_code = ? 
                    ORDER BY analysis_date DESC 
                    LIMIT ?
                ''', (ts_code, limit))
                results = []
                for row in cursor.fetchall():
                    results.append({
                        "report": row[0],
                        "model_name": row[1],
                        "analysis_date": row[2],
                        "created_at": row[3]
                    })
                return results
        except Exception as e:
            print(f"读取 AI 分析历史失败: {e}")
            return []

    def delete_ai_analysis(self, ts_code: str, analysis_date: str) -> bool:
        """删除特定一天的分析记录"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    DELETE FROM ai_analysis_history 
                    WHERE ts_code = ? AND analysis_date = ?
                ''', (ts_code, analysis_date))
                conn.commit()
                return True
        except Exception as e:
            print(f"删除 AI 分析记录失败: {e}")
            return False

    def clear_ai_analysis_history(self, ts_code: str) -> bool:
        """清空个股所有分析历史"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('DELETE FROM ai_analysis_history WHERE ts_code = ?', (ts_code,))
                conn.commit()
                return True
        except Exception as e:
            print(f"清空 AI 分析历史失败: {e}")
            return False
    
    # ==================== 基本面数据缓存 ====================
    
    def get_fundamental(self, ts_code: str, data_type: str) -> Optional[Dict[str, Any]]:
        """
        获取基本面缓存数据
        
        Args:
            ts_code: 股票代码（如 600519.SH）
            data_type: 数据类型（如 'fina_indicator', 'income', 'balance'）
        
        Returns:
            缓存的数据字典，不存在或过期返回 None
        """
        return self._get_cache("fundamental_cache", ts_code, data_type)
    
    def set_fundamental(self, ts_code: str, data_type: str, data: Dict[str, Any], ttl_days: int = None) -> bool:
        """
        设置基本面缓存数据
        
        Args:
            ts_code: 股票代码
            data_type: 数据类型
            data: 要缓存的数据
            ttl_days: 过期天数（默认 30 天）
        """
        if ttl_days is None:
            ttl_days = self.DEFAULT_TTL[CacheType.FUNDAMENTAL]
        return self._set_cache("fundamental_cache", ts_code, data_type, data, ttl_days)
    
    # ==================== 资金流向缓存 ====================
    
    def get_money_flow(self, ts_code: str, trade_date: str) -> Optional[Dict[str, Any]]:
        """获取资金流向缓存"""
        return self._get_cache("money_flow_cache", ts_code, trade_date)
    
    def set_money_flow(self, ts_code: str, trade_date: str, data: Dict[str, Any]) -> bool:
        """设置资金流向缓存（当日有效）"""
        ttl_days = self.DEFAULT_TTL[CacheType.MONEY_FLOW]
        return self._set_cache("money_flow_cache", ts_code, trade_date, data, ttl_days)
    
    # ==================== 每日指标缓存 ====================
    
    def get_daily_basic(self, ts_code: str, trade_date: str) -> Optional[Dict[str, Any]]:
        """获取每日基础指标缓存"""
        return self._get_cache("daily_basic_cache", ts_code, trade_date)
    
    def set_daily_basic(self, ts_code: str, trade_date: str, data: Dict[str, Any]) -> bool:
        """设置每日基础指标缓存"""
        ttl_days = self.DEFAULT_TTL[CacheType.DAILY_BASIC]
        return self._set_cache("daily_basic_cache", ts_code, trade_date, data, ttl_days)
    
    # ==================== 消息面缓存 ====================
    
    def get_news(self, ts_code: str, query_hash: str) -> Optional[List[Dict[str, Any]]]:
        """获取消息面缓存"""
        return self._get_cache("news_cache", ts_code, query_hash)
    
    def set_news(self, ts_code: str, query_hash: str, data: List[Dict[str, Any]], ttl_hours: float = 1) -> bool:
        """设置消息面缓存（默认 1 小时过期）"""
        ttl_days = ttl_hours / 24.0
        return self._set_cache("news_cache", ts_code, query_hash, data, ttl_days)
    
    # ==================== 通用缓存操作 ====================
    
    def _get_cache(self, table: str, key1: str, key2: str) -> Optional[Any]:
        """通用缓存读取"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # 根据表确定主键列名
                if table == "fundamental_cache":
                    col1, col2 = "ts_code", "data_type"
                elif table in ["money_flow_cache", "daily_basic_cache"]:
                    col1, col2 = "ts_code", "trade_date"
                elif table == "news_cache":
                    col1, col2 = "ts_code", "query_hash"
                else:
                    return None
                
                cursor = conn.execute(f'''
                    SELECT data_json, expires_at FROM {table}
                    WHERE {col1} = ? AND {col2} = ?
                ''', (key1, key2))
                
                row = cursor.fetchone()
                if row is None:
                    return None
                
                data_json, expires_at = row
                
                # 检查是否过期
                if datetime.fromisoformat(expires_at) < datetime.now():
                    # 已过期，删除并返回 None
                    conn.execute(f'DELETE FROM {table} WHERE {col1} = ? AND {col2} = ?', (key1, key2))
                    conn.commit()
                    return None
                
                return json.loads(data_json)
        except Exception as e:
            print(f"读取缓存失败 ({table}): {e}")
            return None
    
    def _set_cache(self, table: str, key1: str, key2: str, data: Any, ttl_days: float) -> bool:
        """通用缓存写入"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # 根据表确定主键列名
                if table == "fundamental_cache":
                    col1, col2 = "ts_code", "data_type"
                elif table in ["money_flow_cache", "daily_basic_cache"]:
                    col1, col2 = "ts_code", "trade_date"
                elif table == "news_cache":
                    col1, col2 = "ts_code", "query_hash"
                else:
                    return False
                
                now = datetime.now()
                expires_at = now + timedelta(days=ttl_days)
                
                conn.execute(f'''
                    INSERT OR REPLACE INTO {table} ({col1}, {col2}, data_json, fetched_at, expires_at)
                    VALUES (?, ?, ?, ?, ?)
                ''', (key1, key2, json.dumps(data, ensure_ascii=False, default=str), 
                      now.isoformat(), expires_at.isoformat()))
                conn.commit()
                return True
        except Exception as e:
            print(f"写入缓存失败 ({table}): {e}")
            return False
    
    # ==================== 缓存管理 ====================
    
    def clear_expired(self) -> int:
        """清理所有过期缓存，返回清理的记录数"""
        tables = ["fundamental_cache", "money_flow_cache", "daily_basic_cache", "news_cache"]
        total_deleted = 0
        now = datetime.now().isoformat()
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                for table in tables:
                    cursor = conn.execute(f'DELETE FROM {table} WHERE expires_at < ?', (now,))
                    total_deleted += cursor.rowcount
                conn.commit()
        except Exception as e:
            print(f"清理过期缓存失败: {e}")
        
        return total_deleted
    
    def clear_all(self) -> bool:
        """清空所有缓存（删除并重建数据库）"""
        try:
            if os.path.exists(self.db_path):
                os.remove(self.db_path)
            # 同时删除 WAL 文件
            wal_path = self.db_path + "-wal"
            shm_path = self.db_path + "-shm"
            if os.path.exists(wal_path):
                os.remove(wal_path)
            if os.path.exists(shm_path):
                os.remove(shm_path)
            
            self._init_db()
            return True
        except Exception as e:
            print(f"清空缓存失败: {e}")
            return False
    
    def get_stats(self) -> Dict[str, int]:
        """获取缓存统计信息"""
        tables = ["fundamental_cache", "money_flow_cache", "daily_basic_cache", "news_cache"]
        stats = {}
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                for table in tables:
                    cursor = conn.execute(f'SELECT COUNT(*) FROM {table}')
                    stats[table] = cursor.fetchone()[0]
                
                # 获取数据库文件大小
                stats["db_size_mb"] = round(os.path.getsize(self.db_path) / (1024 * 1024), 2)
        except Exception as e:
            print(f"获取缓存统计失败: {e}")
        
        return stats
    
    def get_cached_codes(self, cache_type: CacheType) -> List[str]:
        """获取指定类型缓存中的所有股票代码"""
        table_map = {
            CacheType.FUNDAMENTAL: "fundamental_cache",
            CacheType.MONEY_FLOW: "money_flow_cache",
            CacheType.DAILY_BASIC: "daily_basic_cache",
            CacheType.NEWS: "news_cache",
        }
        table = table_map.get(cache_type)
        if not table:
            return []
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(f'SELECT DISTINCT ts_code FROM {table}')
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            print(f"获取缓存代码列表失败: {e}")
            return []
