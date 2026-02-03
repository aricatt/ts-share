"""
股票数据服务
基于 Tushare Pro + SQLite 存储
"""
import os
import sqlite3
import tushare as ts
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List
from .cache_service import CacheService
from config import TUSHARE_TOKEN


class StockService:
    """
    股票数据服务（Tushare Pro + SQLite）
    
    优先从本地 SQLite 读取，没有则从 API 获取
    """
    
    def __init__(self, use_cache: bool = True, data_dir: str = "data"):
        self.use_cache = use_cache
        self.cache = CacheService() if use_cache else None
        self.data_dir = data_dir
        self.db_path = os.path.join(data_dir, "stocks.db")
        
        # 初始化 Tushare Pro
        if not TUSHARE_TOKEN:
            raise ValueError("Tushare Token 未配置")
        ts.set_token(TUSHARE_TOKEN)
        self.pro = ts.pro_api()
        
        # 股票信息缓存
        self._stock_info_cache = None
    
    # ==================== 股票列表 ====================
    
    def get_all_stock_codes(self) -> List[str]:
        """获取所有A股股票代码"""
        try:
            df = self.pro.stock_basic(
                list_status='L',
                fields='ts_code,symbol,name,market'
            )
            return df['symbol'].tolist()
        except Exception as e:
            print(f"获取股票列表失败: {e}")
            return []
    
    def get_stock_list(self) -> pd.DataFrame:
        """获取所有股票基本信息"""
        if self._stock_info_cache is not None:
            return self._stock_info_cache
        
        try:
            df = self.pro.stock_basic(
                list_status='L',
                fields='ts_code,symbol,name,area,industry,market,list_date'
            )
            self._stock_info_cache = df
            return df
        except Exception as e:
            print(f"获取股票列表失败: {e}")
            return pd.DataFrame()
    
    def _to_ts_code(self, code: str) -> str:
        """将6位代码转换为 tushare 格式"""
        if '.' in code:
            return code
        if code.startswith(('6', '5', '9')):
            return f"{code}.SH"
        elif code.startswith(('0', '3', '2', '1')):
            return f"{code}.SZ"
        elif code.startswith(('4', '8')):
            return f"{code}.BJ"
        return f"{code}.SZ"
    
    # ==================== 数据库操作 ====================
    
    def _db_exists(self) -> bool:
        """检查数据库是否存在"""
        return os.path.exists(self.db_path)
    
    def _query_db(self, sql: str, params: tuple = None) -> pd.DataFrame:
        """执行数据库查询"""
        if not self._db_exists():
            return pd.DataFrame()
        
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql_query(sql, conn, params=params)
            # 全局防护：移除由于 JOIN 产生的重复列
            if not df.empty:
                df = df.loc[:, ~df.columns.duplicated()]
            return df
    
    # ==================== 涨停股池 ====================
    
    def get_zt_pool(self, date: str) -> pd.DataFrame:
        """
        获取涨停股池
        
        优先从本地 SQLite 查询（涨幅 >= 9.5%）
        """
        cache_key = f"zt_pool_{date}"
        
        if self.cache:
            cached = self.cache.get(cache_key)
            if cached is not None:
                return cached
        
        # 从本地数据库查询
        if self._db_exists():
            sql = '''
                SELECT b.名称, d.*, b.行业
                FROM daily_data d
                LEFT JOIN stock_basic b ON d.代码 = b.代码
                WHERE d.日期 = ? AND d.涨跌幅 >= 9.5
                ORDER BY d.涨跌幅 DESC
            '''
            df = self._query_db(sql, (date,))
            
            if not df.empty:
                # 移除重复列
                df = df.loc[:, ~df.columns.duplicated()]
                if self.cache:
                    is_today = date == datetime.now().strftime("%Y%m%d")
                    self.cache.set(cache_key, df, expire_today=is_today)
                return df
        
        # 本地没有，从 API 获取
        try:
            df = self.pro.limit_list_d(trade_date=date, limit_type='U')
            
            if df is not None and not df.empty:
                df = df.rename(columns={
                    'ts_code': '代码', 'name': '名称',
                    'close': '收盘价', 'pct_chg': '涨跌幅',
                })
                df['代码'] = df['代码'].str[:6]
                
                if self.cache:
                    is_today = date == datetime.now().strftime("%Y%m%d")
                    self.cache.set(cache_key, df, expire_today=is_today)
                
            return df if df is not None else pd.DataFrame()
        except Exception as e:
            print(f"获取涨停数据失败: {e}")
            return pd.DataFrame()
    
    # ==================== 历史K线 ====================
    
    def get_history(self, code: str, days: int = 120) -> Optional[pd.DataFrame]:
        """
        获取个股历史K线
        
        查找顺序：
        1. 本地 SQLite 数据库
        2. Tushare API
        """
        # 1. 从本地数据库获取
        if self._db_exists():
            sql = '''
                SELECT b.名称, d.*, b.行业, b.地区, b.上市日期
                FROM daily_data d
                LEFT JOIN stock_basic b ON d.代码 = b.代码
                WHERE d.代码 = ? 
                ORDER BY d.日期 DESC 
                LIMIT ?
            '''
            df = self._query_db(sql, (code, days))
            
            if not df.empty:
                return df.sort_values('日期')
        
        # 2. 尝试从缓存获取
        cache_key = f"history_{code}_{days}"
        if self.cache:
            cached = self.cache.get(cache_key)
            if cached is not None:
                return cached
        
        # 3. 从 Tushare 获取
        try:
            ts_code = self._to_ts_code(code)
            yesterday = datetime.now() - timedelta(days=1)
            end_date = yesterday.strftime("%Y%m%d")
            start_date = (yesterday - timedelta(days=days)).strftime("%Y%m%d")
            
            df_daily = self.pro.daily(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date
            )
            
            if df_daily is None or df_daily.empty:
                return None
            
            df_basic = self.pro.daily_basic(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date
            )
            
            df = self._merge_daily_data(df_daily, df_basic, code)
            
            if self.cache and df is not None and not df.empty:
                self.cache.set(cache_key, df, expire_today=True)
            
            return df
        except Exception as e:
            print(f"获取 {code} 历史数据失败: {e}")
            return None
    
    def _merge_daily_data(self, df_daily, df_basic, code):
        """合并日线和指标数据"""
        df = df_daily.rename(columns={
            'trade_date': '日期', 'open': '开盘', 'high': '最高',
            'low': '最低', 'close': '收盘', 'pre_close': '昨收',
            'change': '涨跌额', 'pct_chg': '涨跌幅',
            'vol': '成交量', 'amount': '成交额'
        })
        
        if df_basic is not None and not df_basic.empty:
            df_basic = df_basic.rename(columns={
                'trade_date': '日期', 'turnover_rate': '换手率',
                'volume_ratio': '量比', 'pe': 'PE', 'pe_ttm': 'PE_TTM',
                'pb': 'PB', 'total_mv': '总市值', 'circ_mv': '流通市值'
            })
            keep = ['日期', '换手率', '量比', 'PE', 'PE_TTM', 'PB', '总市值', '流通市值']
            available = [c for c in keep if c in df_basic.columns]
            df = df.merge(df_basic[available], on='日期', how='left')
        
        df['代码'] = code
        if 'ts_code' in df.columns:
            df = df.drop(columns=['ts_code'])
        
        return df.sort_values('日期')
    
    # ==================== 实时行情 ====================
    
    def get_realtime_quotes(self) -> pd.DataFrame:
        """获取全A股实时行情"""
        cache_key = "realtime_quotes"
        
        if self.cache:
            cached = self.cache.get(cache_key)
            if cached is not None:
                return cached
        
        try:
            df = ts.realtime_list()
            
            if df is not None and not df.empty:
                df = df.rename(columns={
                    'TS_CODE': '代码', 'NAME': '名称',
                    'PRICE': '最新价', 'PCT_CHG': '涨跌幅',
                    'CHANGE': '涨跌额', 'VOLUME': '成交量',
                    'AMOUNT': '成交额', 'OPEN': '开盘价',
                    'HIGH': '最高价', 'LOW': '最低价'
                })
                if '代码' in df.columns:
                    df['代码'] = df['代码'].str[:6]
            
            if self.cache and df is not None and not df.empty:
                self.cache.set(cache_key, df, expire_today=True)
            
            return df if df is not None else pd.DataFrame()
        except Exception as e:
            print(f"获取实时行情失败: {e}")
            return pd.DataFrame()
    
    # ==================== 数据筛选 ====================
    
    def get_stocks_by_filter(
        self,
        trade_date: str = None,
        min_pct_chg: float = None,
        max_pct_chg: float = None,
        min_pe: float = None,
        max_pe: float = None,
        max_market_cap: float = None,
        min_turnover: float = None,
        limit: int = 100
    ) -> pd.DataFrame:
        """按条件筛选股票（从本地数据库）"""
        if not self._db_exists():
            return pd.DataFrame()
        
        conditions = []
        params = []
        
        if trade_date:
            conditions.append('日期 = ?')
            params.append(trade_date)
        
        if min_pct_chg is not None:
            conditions.append('涨跌幅 >= ?')
            params.append(min_pct_chg)
        
        if max_pct_chg is not None:
            conditions.append('涨跌幅 <= ?')
            params.append(max_pct_chg)
        
        if min_pe is not None:
            conditions.append('PE >= ?')
            params.append(min_pe)
        
        if max_pe is not None:
            conditions.append('PE <= ?')
            params.append(max_pe)
        
        if max_market_cap is not None:
            conditions.append('流通市值 <= ?')
            params.append(max_market_cap * 1e4)
        
        if min_turnover is not None:
            conditions.append('换手率 >= ?')
            params.append(min_turnover)
        
        where_clause = ' AND '.join(conditions) if conditions else '1=1'
        sql = f'''
            SELECT b.名称, d.*, b.行业, b.地区
            FROM daily_data d
            LEFT JOIN stock_basic b ON d.代码 = b.代码
            WHERE {where_clause.replace('日期', 'd.日期').replace('涨跌幅', 'd.涨跌幅').replace('PE', 'd.PE').replace('流通市值', 'd.流通市值').replace('换手率', 'd.换手率')}
            ORDER BY d.日期 DESC, d.涨跌幅 DESC
            LIMIT ?
        '''
        params.append(limit)
        
        return self._query_db(sql, tuple(params))
    
    def query(self, sql: str, params: tuple = None) -> pd.DataFrame:
        """执行自定义 SQL 查询"""
        return self._query_db(sql, params)
    
    # ==================== 交易日历 ====================
    
    def get_trade_cal(self, start_date: str, end_date: str) -> pd.DataFrame:
        """获取交易日历"""
        return self.pro.trade_cal(
            exchange='SSE',
            start_date=start_date,
            end_date=end_date
        )
    
    def is_trading_day(self, date: str = None) -> bool:
        """判断是否为交易日"""
        if date is None:
            date = datetime.now().strftime("%Y%m%d")
        df = self.get_trade_cal(start_date=date, end_date=date)
        if df.empty:
            return False
        return df.iloc[0]['is_open'] == 1
    
    def get_last_trading_day(self) -> str:
        """获取最近交易日"""
        today = datetime.now()
        for i in range(10):
            date = (today - timedelta(days=i)).strftime("%Y%m%d")
            if self.is_trading_day(date):
                return date
        return today.strftime("%Y%m%d")
    
    # ==================== 数据源路由 ====================
    
    def get_data_by_source(self, source: str, date: str = None) -> pd.DataFrame:
        """根据数据源类型获取数据"""
        if source == "zt_pool":
            if date is None:
                date = datetime.now().strftime("%Y%m%d")
            return self.get_zt_pool(date)
        elif source == "all_stocks":
            return self.get_realtime_quotes()
        elif source == "historical_zt":
            return self.get_historical_zt_stocks(date=date, days=90)
        else:
            raise ValueError(f"未知数据源类型: {source}")
    
    def get_historical_zt_stocks(self, date: str = None, days: int = 90) -> pd.DataFrame:
        """获取历史涨停过的股票极其目标日期的行情"""
        if not self._db_exists():
            return pd.DataFrame()
        
        # 1. 找出过去 N 天涨停过的代码池
        if not date:
            date = datetime.now().strftime("%Y%m%d")
            
        sql_codes = '''
            SELECT DISTINCT 代码 FROM daily_data 
            WHERE 涨跌幅 >= 9.5 AND 日期 <= ? AND 日期 >= ?
        '''
        start_date = (datetime.strptime(date, "%Y%m%d") - timedelta(days=days)).strftime("%Y%m%d")
        codes_df = self._query_db(sql_codes, (date, start_date))
        
        if codes_df.empty:
            return pd.DataFrame()
            
        codes = codes_df['代码'].tolist()
        codes_placeholder = ', '.join(['?'] * len(codes))
        
        # 2. 获取目标日期的完整指标
        sql_data = f'''
            SELECT b.名称, d.*, b.行业, b.地区
            FROM daily_data d
            LEFT JOIN stock_basic b ON d.代码 = b.代码
            WHERE d.日期 = ? AND d.代码 IN ({codes_placeholder})
        '''
        df = self._query_db(sql_data, (date, *codes))
        if not df.empty:
            df = df.loc[:, ~df.columns.duplicated()]
        return df
