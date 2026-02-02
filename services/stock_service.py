"""
股票数据服务
封装 AkShare 接口，提供统一的数据获取方法
"""
import os
import akshare as ak
import pandas as pd
from datetime import datetime
from typing import Optional
from .cache_service import CacheService


class StockService:
    """股票数据服务"""
    
    def __init__(self, use_cache: bool = True):
        self.use_cache = use_cache
        self.cache = CacheService() if use_cache else None
    
    def get_zt_pool(self, date: str) -> pd.DataFrame:
        """
        获取涨停股池
        
        Args:
            date: 日期，格式 YYYYMMDD
        
        Returns:
            涨停股 DataFrame
        """
        cache_key = f"zt_pool_{date}"
        
        # 尝试从缓存获取
        if self.cache:
            cached = self.cache.get(cache_key)
            if cached is not None:
                return cached
        
        # 从 AkShare 获取
        try:
            df = ak.stock_zt_pool_em(date=date)
            
            # 缓存结果（非当日数据永久缓存）
            if self.cache and not df.empty:
                is_today = date == datetime.now().strftime("%Y%m%d")
                self.cache.set(cache_key, df, expire_today=is_today)
            
            return df
        except Exception as e:
            print(f"获取涨停数据失败: {e}")
            return pd.DataFrame()
    
    def get_history(self, code: str, days: int = 120) -> Optional[pd.DataFrame]:
        """
        获取个股历史K线（优先从本地获取）
        
        查找顺序：
        1. 本地 Parquet 文件（data/stocks/{code}.parquet）
        2. AkShare API（如果本地没有）
        
        Args:
            code: 股票代码
            days: 获取天数
        
        Returns:
            K线 DataFrame
        """
        # 1. 优先从本地 Parquet 获取
        local_path = os.path.join("data", "stocks", f"{code}.parquet")
        if os.path.exists(local_path):
            try:
                df = pd.read_parquet(local_path)
                # 只返回最近 days 天的数据
                if len(df) > days:
                    df = df.tail(days)
                return df
            except Exception as e:
                print(f"读取本地数据失败: {e}")
        
        # 2. 本地没有，尝试从缓存获取
        cache_key = f"history_{code}_{days}"
        if self.cache:
            cached = self.cache.get(cache_key)
            if cached is not None:
                return cached
        
        # 3. 从 AkShare 获取
        try:
            from datetime import timedelta
            yesterday = datetime.now() - timedelta(days=1)
            end_date = yesterday.strftime("%Y%m%d")
            start_date = (yesterday - timedelta(days=days)).strftime("%Y%m%d")
            
            df = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"
            )
            
            if self.cache and df is not None and not df.empty:
                self.cache.set(cache_key, df, expire_today=True)
            
            return df
        except Exception as e:
            print(f"获取 {code} 历史数据失败: {e}")
            return None
    
    def get_realtime_quotes(self) -> pd.DataFrame:
        """
        获取全A股实时行情
        
        Returns:
            实时行情 DataFrame
        """
        cache_key = "realtime_quotes"
        
        # 尝试从缓存获取（短时间缓存，避免频繁请求）
        if self.cache:
            cached = self.cache.get(cache_key)
            if cached is not None:
                return cached
        
        try:
            df = ak.stock_zh_a_spot_em()
            
            # 缓存5分钟
            if self.cache and not df.empty:
                self.cache.set(cache_key, df, expire_today=True)
            
            return df
        except Exception as e:
            print(f"获取实时行情失败: {e}")
            return pd.DataFrame()
    
    def get_data_by_source(self, source: str, date: str = None) -> pd.DataFrame:
        """
        根据数据源类型获取数据
        
        Args:
            source: 数据源类型 'zt_pool' | 'all_stocks' | 'historical_zt'
            date: 日期（仅 zt_pool 需要）
        
        Returns:
            股票数据 DataFrame
        """
        if source == "zt_pool":
            if date is None:
                from datetime import datetime
                date = datetime.now().strftime("%Y%m%d")
            return self.get_zt_pool(date)
        elif source == "all_stocks":
            return self.get_realtime_quotes()
        elif source == "historical_zt":
            return self.get_historical_zt_stocks(days=90)
        else:
            raise ValueError(f"未知数据源类型: {source}")
    
    def get_historical_zt_stocks(self, days: int = 90) -> pd.DataFrame:
        """
        获取历史涨停过的股票（用于龙回头策略）
        
        思路：
        1. 获取过去N天的涨停股池数据
        2. 合并去重，得到"曾经涨停过"的股票代码列表
        3. 获取这些股票的当前行情
        
        Args:
            days: 回溯天数
        
        Returns:
            历史涨停股的当前行情 DataFrame
        """
        from datetime import timedelta
        import streamlit as st
        
        cache_key = f"historical_zt_{days}"
        
        # 尝试从缓存获取
        if self.cache:
            cached = self.cache.get(cache_key)
            if cached is not None:
                return cached
        
        # 收集历史涨停股代码
        all_codes = set()
        end_date = datetime.now()
        
        # 每隔几天采样一次，减少请求次数
        sample_interval = 3  # 每3天采样一次
        
        for i in range(0, days, sample_interval):
            date = (end_date - timedelta(days=i)).strftime("%Y%m%d")
            try:
                df = self.get_zt_pool(date)
                if not df.empty and '代码' in df.columns:
                    all_codes.update(df['代码'].tolist())
            except Exception as e:
                continue  # 跳过非交易日
        
        if not all_codes:
            return pd.DataFrame()
        
        print(f"历史涨停股数量: {len(all_codes)}")
        
        # 获取这些股票的当前行情
        # 方案：获取强势股池（自动找到最近有数据的交易日）
        strong_df = pd.DataFrame()
        
        for i in range(1, 15):  # 最多往前找15天
            date_str = (end_date - timedelta(days=i)).strftime("%Y%m%d")
            try:
                temp_df = ak.stock_zt_pool_strong_em(date=date_str)
                if not temp_df.empty:
                    strong_df = temp_df
                    print(f"使用 {date_str} 的强势股池数据")
                    break
            except Exception as e:
                continue
        
        if strong_df.empty:
            # 如果强势股池没数据，直接返回历史涨停股代码列表（需要后续获取行情）
            print("强势股池无数据，返回历史涨停代码列表")
            return pd.DataFrame({'代码': list(all_codes)})
        
        # 过滤出在历史涨停列表中的股票
        if '代码' in strong_df.columns:
            strong_df = strong_df[strong_df['代码'].isin(all_codes)]
        
        if self.cache and not strong_df.empty:
            self.cache.set(cache_key, strong_df, expire_today=True)
        
        return strong_df
