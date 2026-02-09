"""
股票数据服务
基于 Tushare Pro + SQLite 存储
"""
import os
import sqlite3
import tushare as ts
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List
from .cache_service import CacheService
from .analysis_cache_service import AnalysisCacheService, CacheType
from config import TUSHARE_TOKEN, MARKET_CAP_UNIT


class StockService:
    """
    股票数据服务（Tushare Pro + SQLite）
    
    优先从本地 SQLite 读取，没有则从 API 获取
    """
    
    def __init__(self, use_cache: bool = True, data_dir: str = "data"):
        self.use_cache = use_cache
        self.cache = CacheService() if use_cache else None
        self.analysis_cache = AnalysisCacheService(data_dir=data_dir) if use_cache else None
        self.data_dir = data_dir
        self.db_path = os.path.join(data_dir, "stocks.db")
        
        # 初始化 Tushare Pro
        if not TUSHARE_TOKEN:
            raise ValueError("Tushare Token 未配置")
        ts.set_token(TUSHARE_TOKEN)
        self.pro = ts.pro_api()
        
        # 股票信息缓存
        self._stock_info_cache = None
        
        # 确保基础表存在
        self._init_db()

    def _init_db(self):
        """初始化必要的数据库表（主要是收藏表）"""
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir, exist_ok=True)
            
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS collected_stocks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    代码 TEXT NOT NULL,
                    名称 TEXT,
                    收藏日期 TEXT NOT NULL,
                    策略名称 TEXT NOT NULL,
                    备注 TEXT,
                    UNIQUE(代码, 策略名称)
                )
            ''')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_collect_code ON collected_stocks (代码)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_collect_strategy ON collected_stocks (策略名称)')
            conn.commit()
    
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
    
    def search_stocks(self, query: str, limit: int = 10) -> pd.DataFrame:
        """
        模糊搜索股票（按代码或名称）
        
        Args:
            query: 搜索关键词
            limit: 返回结果数量限制
            
        Returns:
            满足条件的股票基本信息 DataFrame
        """
        if not query:
            return pd.DataFrame()
            
        # 1. 优先尝试从本地数据库搜索
        if self._db_exists():
            sql = '''
                SELECT 代码, 名称, 行业, 市场
                FROM stock_basic
                WHERE 代码 LIKE ? OR 名称 LIKE ?
                LIMIT ?
            '''
            like_query = f"%{query}%"
            df = self._query_db(sql, (like_query, like_query, limit))
            if not df.empty:
                return df
                
        # 2. 如果数据库没有或没搜到，尝试从在线列表搜
        df_list = self.get_stock_list()
        if not df_list.empty:
            # 过滤
            mask = df_list['symbol'].str.contains(query, case=False) | df_list['name'].str.contains(query, case=False)
            df_match = df_list[mask].head(limit).copy()
            if not df_match.empty:
                return df_match.rename(columns={'symbol': '代码', 'name': '名称', 'industry': '行业', 'market': '市场'})
                
        return pd.DataFrame()

    def get_stock_name(self, code: str) -> str:
        """根据代码获取股票名称"""
        if not code:
            return "未知"
            
        # 1. 优先从内存缓存找
        df_list = self.get_stock_list()
        if not df_list.empty:
            # 去掉后缀
            symbol = code.split('.')[0]
            match = df_list[df_list['symbol'] == symbol]
            if not match.empty:
                return match.iloc[0]['name']
                
        # 2. 从本地数据库找
        if self._db_exists():
            sql = "SELECT 名称 FROM stock_basic WHERE 代码 = ? LIMIT 1"
            df = self._query_db(sql, (code.split('.')[0],))
            if not df.empty:
                return df.iloc[0]['名称']
                
        return code

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
            params.append(max_market_cap * MARKET_CAP_UNIT)
        
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
            # 如果提供了日期，优先从本地数据库获取全量数据
            if date:
                df_db = self.get_daily_all(date)
                if not df_db.empty:
                    return df_db
            # 否则尝试实时列表（兜底）
            return self.get_realtime_quotes()
        elif source == "historical_zt":
            return self.get_historical_zt_stocks(date=date, days=90)
        else:
            raise ValueError(f"未知数据源类型: {source}")

    def get_daily_all(self, date: str) -> pd.DataFrame:
        """从本地数据库获取指定日期的全量股票数据记录"""
        if not self._db_exists():
            return pd.DataFrame()
            
        sql = '''
            SELECT b.名称, d.*, b.行业, b.市场
            FROM daily_data d
            LEFT JOIN stock_basic b ON d.代码 = b.代码
            WHERE d.日期 = ?
            ORDER BY d.涨跌幅 DESC
        '''
        df = self._query_db(sql, (date,))
        return df
    
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
    # ==================== 股票收藏控制 ====================
    
    def collect_stock(self, code: str, name: str, rule_name: str, remark: str = "") -> bool:
        """收藏股票"""
        if not self._db_exists():
            return False
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                conn.execute('''
                    INSERT OR REPLACE INTO collected_stocks (代码, 名称, 收藏日期, 策略名称, 备注)
                    VALUES (?, ?, ?, ?, ?)
                ''', (code, name, now, rule_name, remark))
                conn.commit()
                return True
        except Exception as e:
            print(f"收藏股票失败: {e}")
            return False

    def remove_collected_stock(self, code: str, rule_name: str) -> bool:
        """取消收藏"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('DELETE FROM collected_stocks WHERE 代码 = ? AND 策略名称 = ?', (code, rule_name))
                conn.commit()
                return True
        except Exception as e:
            print(f"取消收藏失败: {e}")
            return False

    def get_collected_stocks(self, rule_name: str = None) -> pd.DataFrame:
        """获取收藏列表"""
        sql = "SELECT * FROM collected_stocks"
        params = []
        if rule_name:
            sql += " WHERE 策略名称 = ?"
            params.append(rule_name)
        
        sql += " ORDER BY 收藏日期 DESC"
        df_fav = self._query_db(sql, tuple(params))
        
        if df_fav.empty:
            return df_fav
            
        # 尝试关联最新的行情数据（如果有）
        try:
            codes = df_fav['代码'].unique().tolist()
            placeholder = ', '.join(['?'] * len(codes))
            # 获取每个代码最近的一条记录
            sql_latest = f'''
                SELECT d.*, b.名称 as 真实名称, b.行业
                FROM daily_data d
                LEFT JOIN stock_basic b ON d.代码 = b.代码
                WHERE d.代码 IN ({placeholder})
                AND d.日期 = (SELECT MAX(日期) FROM daily_data)
            '''
            df_latest = self._query_db(sql_latest, tuple(codes))
            
            if not df_latest.empty:
                # 合并收藏信息和最新行情
                res = df_fav.merge(df_latest, on='代码', how='left', suffixes=('', '_latest'))
                # 补全名称（以 basic 表为准）
                if '真实名称' in res.columns:
                    res['名称'] = res['真实名称'].fillna(res['名称'])
                return res
        except:
            pass
            
        return df_fav

    def is_collected(self, code: str, rule_name: str) -> bool:
        """检查是否已收藏"""
        df = self._query_db(
            "SELECT 1 FROM collected_stocks WHERE 代码 = ? AND 策略名称 = ?",
            (code, rule_name)
        )
        return not df.empty

    # ==================== 分析数据 (带独立缓存) ====================

    def get_fundamental(self, ts_code: str, data_type: str = 'fina_indicator') -> Optional[pd.DataFrame]:
        """
        获取基本面数据（带缓存）
        data_type: fina_indicator(财务指标), income(利润表), balancesheet(资产负债表), cashflow(现金流量表)
        """
        if self.analysis_cache:
            cached = self.analysis_cache.get_fundamental(ts_code, data_type)
            if cached is not None:
                return pd.DataFrame(cached)

        # 从 API 获取
        try:
            if data_type == 'fina_indicator':
                df = self.pro.fina_indicator(ts_code=ts_code)
            elif data_type == 'income':
                df = self.pro.income(ts_code=ts_code)
            elif data_type == 'balancesheet':
                df = self.pro.balancesheet(ts_code=ts_code)
            elif data_type == 'cashflow':
                df = self.pro.cashflow(ts_code=ts_code)
            else:
                return None

            if df is not None and not df.empty:
                if self.analysis_cache:
                    # 存储为 dict 列表
                    self.analysis_cache.set_fundamental(ts_code, data_type, df.to_dict('records'))
                return df
        except Exception as e:
            print(f"获取基本面数据失败 ({ts_code}, {data_type}): {e}")
        
        return None

    def get_money_flow_cached(self, ts_code: str, trade_date: str, max_retries: int = 5) -> Optional[pd.DataFrame]:
        """
        获取资金流向数据（带缓存）
        
        如果指定日期没有数据（如交易日尚未收盘），会尝试向前回溯获取最近有数据的一天。
        """
        current_date = trade_date
        
        # 循环回溯，直到找到数据或达到最大尝试次数
        for i in range(max_retries):
            # 1. 查缓存
            if self.analysis_cache:
                cached = self.analysis_cache.get_money_flow(ts_code, current_date)
                if cached is not None:
                    df = pd.DataFrame(cached)
                    # 在结果中注入实际数据的日期，方便 UI 展示
                    df['_actual_date'] = current_date
                    return df

            # 2. 查 API
            try:
                df = self.pro.moneyflow(ts_code=ts_code, trade_date=current_date)
                if df is not None and not df.empty:
                    if self.analysis_cache:
                        self.analysis_cache.set_money_flow(ts_code, current_date, df.to_dict('records'))
                    df['_actual_date'] = current_date
                    return df
            except Exception as e:
                # 检查是否是积分不足错误
                error_msg = str(e)
                if "权限" in error_msg or "积分" in error_msg or "40001" in error_msg:
                    print(f"❌ Tushare 积分不足，无法获取资金流向: {e}")
                    return None
                print(f"⚠️ 获取 {current_date} 资金流失败: {e}")

            # 3. 没找到，向前推一天再试
            dt = datetime.strptime(current_date, "%Y%m%d")
            current_date = (dt - timedelta(days=1)).strftime("%Y%m%d")
            
        return None

    def get_daily_basic_cached(self, ts_code: str, trade_date: str) -> Optional[pd.DataFrame]:
        """获取每日指标（PE/PB等，带缓存）"""
        if self.analysis_cache:
            cached = self.analysis_cache.get_daily_basic(ts_code, trade_date)
            if cached is not None:
                return pd.DataFrame(cached)

        try:
            df = self.pro.daily_basic(ts_code=ts_code, trade_date=trade_date)
            if df is not None and not df.empty:
                if self.analysis_cache:
                    self.analysis_cache.set_daily_basic(ts_code, trade_date, df.to_dict('records'))
                return df
        except Exception as e:
            print(f"获取每日指标失败 ({ts_code}, {trade_date}): {e}")
            
        return None

    def get_stock_news(self, ts_code: str, days: int = 30) -> Optional[pd.DataFrame]:
        """
        获取个股最新新闻（带缓存）
        使用 AkShare 获取东方财富新闻，并增加防封策略
        """
        import time
        import random
        
        # 提取 6 位代码
        symbol = ts_code.split('.')[0]
        query_hash = f"ak_news_{days}d"
        
        # 1. 优先读缓存（缓存 24 小时，极大降低请求频率）
        if self.analysis_cache:
            cached = self.analysis_cache.get_news(ts_code, query_hash)
            if cached is not None:
                return pd.DataFrame(cached)

        try:
            # 2. 礼貌性延迟（如果是自动化脚本触发，这里能起到保护作用）
            # 模拟真实用户行为，随机等待 0.5 - 1.5 秒
            time.sleep(random.uniform(0.5, 1.5))
            
            # 3. 使用 AkShare 获取新闻
            df = ak.stock_news_em(symbol=symbol)
            
            if df is not None and not df.empty:
                df_res = df.rename(columns={
                    '发布时间': 'ann_date',
                    '新闻标题': 'title',
                    '新闻来源': 'ann_type',
                    '新闻链接': 'url'
                })
                
                if self.analysis_cache:
                    # 缓存 24 小时
                    self.analysis_cache.set_news(ts_code, query_hash, df_res.to_dict('records'), ttl_hours=24)
                return df_res
        except Exception as e:
            # 如果被封或网络问题，返回 None 但不崩溃
            print(f"⚠️ AkShare 获取新闻受限或失败 ({symbol}): {e}")
            
        return None

    def get_ai_analysis_data(self, code: str) -> dict:
        """
        为 AI 代理聚合所有维度的分析报告数据
        """
        ts_code = self._to_ts_code(code)
        last_date = self.get_last_trading_day()
        
        # 1. 基础行情（最近 10 天）
        hist = self.get_history(code, days=10)
        hist_data = hist.to_dict('records') if hist is not None else []
        
        # 2. 财务指标
        fina = self.get_fundamental(ts_code, 'fina_indicator')
        fina_data = fina.iloc[0].to_dict() if fina is not None and not fina.empty else {}
        
        # 3. 资金流向（最近一天）
        money = self.get_money_flow_cached(ts_code, last_date)
        money_data = money.iloc[0].to_dict() if money is not None and not money.empty else {}
        
        # 4. 最新消息
        news = self.get_stock_news(ts_code, days=15)
        news_list = news['title'].tolist() if news is not None and not news.empty else []
        
        return {
            "ts_code": ts_code,
            "stock_name": self.get_stock_name(code),
            "date": last_date,
            "recent_prices": hist_data,
            "key_financials": fina_data,
            "money_flow": money_data,
            "latest_news": news_list[:10] # 取最新的 10 条
        }
