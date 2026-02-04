"""
本地数据同步服务
基于 Tushare Pro + SQLite 存储

优势：
- 单文件存储，便于备份迁移
- 支持 SQL 查询，筛选灵活
- 批量写入高效
- Python 内置支持
"""
import os
import json
import sqlite3
import tushare as ts
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import time
import threading
import fcntl

from config import TUSHARE_TOKEN


# 全局线程锁
_sync_lock = threading.Lock()


class DataSyncService:
    """
    本地数据同步服务（SQLite 存储版）
    
    存储结构：
        data/
        ├── stocks.db          # SQLite 数据库
        └── metadata.json      # 同步元数据
    
    数据表：
        daily_data: 日期, 代码, 开盘, 最高, 最低, 收盘, 涨跌幅, 成交量, 换手率, PE, PB, 市值...
    """
    
    _is_syncing = False
    _sync_start_time = None
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.db_path = os.path.join(data_dir, "stocks.db")
        self.metadata_path = os.path.join(data_dir, "metadata.json")
        self.lock_file_path = os.path.join(data_dir, ".sync.lock")
        self._stop_requested = False
        self._lock_fd = None
        
        # 初始化 Tushare Pro
        if not TUSHARE_TOKEN:
            raise ValueError("Tushare Token 未配置")
        ts.set_token(TUSHARE_TOKEN)
        self.pro = ts.pro_api()
        
        # 创建目录和数据库
        os.makedirs(data_dir, exist_ok=True)
        self._init_database()
    
    def _init_database(self):
        """初始化数据库表结构"""
        with sqlite3.connect(self.db_path) as conn:
            # 1. 日线数据表
            conn.execute('''
                CREATE TABLE IF NOT EXISTS daily_data (
                    日期 TEXT NOT NULL,
                    代码 TEXT NOT NULL,
                    名称 TEXT,
                    开盘 REAL,
                    最高 REAL,
                    最低 REAL,
                    收盘 REAL,
                    昨收 REAL,
                    涨跌额 REAL,
                    涨跌幅 REAL,
                    成交量 REAL,
                    成交额 REAL,
                    换手率 REAL,
                    量比 REAL,
                    PE REAL,
                    PE_TTM REAL,
                    PB REAL,
                    总市值 REAL,
                    流通市值 REAL,
                    总股本 REAL,
                    流通股本 REAL,
                    复权因子 REAL,
                    qfq_开盘 REAL,
                    qfq_最高 REAL,
                    qfq_最低 REAL,
                    qfq_收盘 REAL,
                    ma5 REAL,
                    ma10 REAL,
                    ma20 REAL,
                    vma5 REAL,
                    vma10 REAL,
                    vma20 REAL,
                    PRIMARY KEY (日期, 代码)
                )
            ''')
            
            # 2. 股票基础信息表 (用于名称关联)
            conn.execute('''
                CREATE TABLE IF NOT EXISTS stock_basic (
                    代码 TEXT PRIMARY KEY,
                    名称 TEXT,
                    行业 TEXT,
                    区域 TEXT,
                    市场 TEXT,
                    上市日期 TEXT
                )
            ''')

            # 3. 收藏股票表
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
            
            # 创建索引
            conn.execute('CREATE INDEX IF NOT EXISTS idx_daily_date ON daily_data (日期)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_daily_code ON daily_data (代码)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_pct_chg ON daily_data(涨跌幅)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_collect_code ON collected_stocks (代码)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_collect_strategy ON collected_stocks (策略名称)')
            
            # 动态检查并添加缺失的列（适配已有数据库升级）
            cursor = conn.execute("PRAGMA table_info(daily_data)")
            existing_cols = {row[1] for row in cursor.fetchall()}
            
            # 需要存在的列
            target_cols = {
                '名称': 'TEXT',
                '复权因子': 'REAL',
                'qfq_开盘': 'REAL',
                'qfq_最高': 'REAL',
                'qfq_最低': 'REAL',
                'qfq_收盘': 'REAL',
                'ma5': 'REAL',
                'ma10': 'REAL',
                'ma20': 'REAL',
                'ma60': 'REAL',
                'vma5': 'REAL',
                'vma10': 'REAL',
                'vma20': 'REAL'
            }
            
            for col, col_type in target_cols.items():
                if col not in existing_cols:
                    print(f"🔧 正在升级数据库：添加列 {col}...")
                    try:
                        conn.execute(f"ALTER TABLE daily_data ADD COLUMN {col} {col_type}")
                    except Exception as e:
                        print(f"⚠️ 添加列 {col} 失败: {e}")
            
            conn.commit()
    
    # ==================== 锁机制 ====================
    
    def _acquire_lock(self) -> bool:
        if not _sync_lock.acquire(blocking=False):
            return False
        try:
            self._lock_fd = open(self.lock_file_path, 'w')
            fcntl.flock(self._lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            lock_info = {"pid": os.getpid(), "start_time": datetime.now().isoformat()}
            self._lock_fd.write(json.dumps(lock_info))
            self._lock_fd.flush()
            DataSyncService._is_syncing = True
            DataSyncService._sync_start_time = datetime.now()
            return True
        except (IOError, OSError):
            _sync_lock.release()
            return False
    
    def _release_lock(self):
        DataSyncService._is_syncing = False
        DataSyncService._sync_start_time = None
        if self._lock_fd:
            try:
                fcntl.flock(self._lock_fd.fileno(), fcntl.LOCK_UN)
                self._lock_fd.close()
                if os.path.exists(self.lock_file_path):
                    os.remove(self.lock_file_path)
            except:
                pass
            self._lock_fd = None
        try:
            _sync_lock.release()
        except:
            pass
    
    def is_syncing(self) -> bool:
        if DataSyncService._is_syncing:
            return True
        if os.path.exists(self.lock_file_path):
            try:
                with open(self.lock_file_path, 'r') as f:
                    lock_info = json.load(f)
                    pid = lock_info.get("pid")
                    if pid:
                        try:
                            os.kill(pid, 0)
                            return True
                        except OSError:
                            pass
            except:
                pass
        return False
    
    def get_sync_status(self) -> dict:
        if self.is_syncing():
            start_time = DataSyncService._sync_start_time
            elapsed = (datetime.now() - start_time).total_seconds() if start_time else 0
            return {"is_syncing": True, "elapsed_seconds": int(elapsed)}
        return {"is_syncing": False, "elapsed_seconds": 0}
    
    def request_stop(self):
        self._stop_requested = True
    
    # ==================== 元数据 ====================
    
    def get_metadata(self) -> dict:
        if os.path.exists(self.metadata_path):
            with open(self.metadata_path, 'r') as f:
                return json.load(f)
        return {"last_sync_date": None, "total_stocks": 0, "days": 0}
    
    def save_metadata(self, metadata: dict):
        with open(self.metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
    
    # ==================== 交易日历 ====================
    
    def recompute_technical_indicators(self):
        """重新计算所有股票的前复权价格和均线指标"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # 1. 加载所有行情数据
                print("   加载数据...")
                df = pd.read_sql_query("SELECT * FROM daily_data ORDER BY 代码, 日期", conn)
                
                if df.empty:
                    return
                
                print(f"   计算指标 ({len(df)} 条记录)...")
                # 确保代码顺序
                df = df.sort_values(['代码', '日期'])
                
                # 计算前复权收盘价
                # 前复权原理：P' = P * (Current_Factor / Last_Factor)
                
                # 获取每只股票最后一次有效的复权因子作为基准
                # 必须先剔除 NULL 才能找到真正的“最后”一个因子
                valid_factors = df[df['复权因子'].notnull()].groupby('代码')['复权因子'].last()
                df['base_factor'] = df['代码'].map(valid_factors)
                
                # 只有当原始价格和当前复权因子都存在时才计算
                mask = df['收盘'].notnull() & df['复权因子'].notnull() & df['base_factor'].notnull()
                
                # 比例系数
                df['adj_ratio'] = 1.0
                df.loc[mask, 'adj_ratio'] = df.loc[mask, '复权因子'] / df.loc[mask, 'base_factor']
                
                # 前复权 OHLC
                df['qfq_开盘'] = (df['开盘'] * df['adj_ratio']).round(2)
                df['qfq_最高'] = (df['最高'] * df['adj_ratio']).round(2)
                df['qfq_最低'] = (df['最低'] * df['adj_ratio']).round(2)
                df['qfq_收盘'] = (df['收盘'] * df['adj_ratio']).round(2)
                
                # 对于还是 NULL 的（比如没有复权因子的品种），使用原始价格
                df['qfq_收盘'] = df['qfq_收盘'].fillna(df['收盘'])
                df['qfq_开盘'] = df['qfq_开盘'].fillna(df['开盘'])
                df['qfq_最高'] = df['qfq_最高'].fillna(df['最高'])
                df['qfq_最低'] = df['qfq_最低'].fillna(df['最低'])
                
                # 计算均线 (基于前复权收盘价)
                gp = df.groupby('代码')['qfq_收盘']
                df['ma5'] = gp.transform(lambda x: x.rolling(5).mean()).round(2)
                df['ma10'] = gp.transform(lambda x: x.rolling(10).mean()).round(2)
                df['ma20'] = gp.transform(lambda x: x.rolling(20).mean()).round(2)
                df['ma60'] = gp.transform(lambda x: x.rolling(60).mean()).round(2)
                
                # 计算均量
                gv = df.groupby('代码')['成交量']
                df['vma5'] = gv.transform(lambda x: x.rolling(5).mean()).round(0)
                df['vma10'] = gv.transform(lambda x: x.rolling(10).mean()).round(0)
                df['vma20'] = gv.transform(lambda x: x.rolling(20).mean()).round(0)
                
                # 2. 回写数据库
                print("   保存结果...")
                update_cols = ['qfq_开盘', 'qfq_最高', 'qfq_最低', 'qfq_收盘', 'ma5', 'ma10', 'ma20', 'ma60', 'vma5', 'vma10', 'vma20', '日期', '代码']
                df_update = df[update_cols]
                
                # 使用事务批量更新
                cursor = conn.cursor()
                sql = '''
                    UPDATE daily_data 
                    SET qfq_开盘 = ?, qfq_最高 = ?, qfq_最低 = ?, qfq_收盘 = ?, 
                        ma5 = ?, ma10 = ?, ma20 = ?, ma60 = ?, vma5 = ?, vma10 = ?, vma20 = ?
                    WHERE 日期 = ? AND 代码 = ?
                '''
                data = [tuple(x) for x in df_update.values]
                cursor.executemany(sql, data)
                conn.commit()
                print("✅ 技术指标计算完成")
                
        except Exception as e:
            print(f"❌ 计算技术指标失败: {e}")
            import traceback
            traceback.print_exc()

    # ==================== 股票列表与基础信息 ====================
    
    def sync_stock_basic(self) -> bool:
        """同步股票基础信息 (名称、行业等)"""
        try:
            print("正在从 Tushare 获取全市场股票基础信息...")
            df = self.pro.stock_basic(
                list_status='L',
                fields='symbol,name,area,industry,market,list_date,delist_date,list_status'
            )
            
            if df is None or df.empty:
                return False
                
            df = df.rename(columns={
                'symbol': '代码',
                'name': '名称',
                'area': '地区',
                'industry': '行业',
                'market': '市场',
                'list_date': '上市日期',
                'delist_date': '退市日期',
                'list_status': '状态'
            })
            
            with sqlite3.connect(self.db_path) as conn:
                df.to_sql('stock_basic', conn, if_exists='replace', index=False)
                conn.commit()
            
            print(f"✅ 股票基础信息同步完成，共 {len(df)} 只股票")
            return True
        except Exception as e:
            print(f"❌ 同步股票基础信息失败: {e}")
            return False

    def get_trading_days(self, start_date: str, end_date: str) -> List[str]:
        """获取交易日列表"""
        try:
            df = self.pro.trade_cal(
                exchange='SSE',
                start_date=start_date,
                end_date=end_date,
                is_open='1'
            )
            return sorted(df['cal_date'].tolist())
        except Exception as e:
            print(f"获取交易日历失败: {e}")
            return []
    
    # ==================== 按日期批量获取 ====================
    
    def fetch_daily_by_date(self, trade_date: str) -> pd.DataFrame:
        """按日期获取全市场日线行情"""
        try:
            df = self.pro.daily(trade_date=trade_date)
            if df is not None and not df.empty:
                df['代码'] = df['ts_code'].str[:6]
                df = df.rename(columns={
                    'trade_date': '日期',
                    'open': '开盘', 'high': '最高', 'low': '最低',
                    'close': '收盘', 'pre_close': '昨收',
                    'change': '涨跌额', 'pct_chg': '涨跌幅',
                    'vol': '成交量', 'amount': '成交额'
                })
                df = df.drop(columns=['ts_code'], errors='ignore')
            return df
        except Exception as e:
            print(f"获取 {trade_date} daily 失败: {e}")
            return pd.DataFrame()
    
    def fetch_daily_basic_by_date(self, trade_date: str) -> pd.DataFrame:
        """按日期获取全市场每日指标"""
        try:
            df = self.pro.daily_basic(trade_date=trade_date)
            if df is not None and not df.empty:
                df['代码'] = df['ts_code'].str[:6]
                df = df.rename(columns={
                    'trade_date': '日期',
                    'turnover_rate': '换手率',
                    'volume_ratio': '量比',
                    'pe': 'PE', 'pe_ttm': 'PE_TTM', 'pb': 'PB',
                    'total_share': '总股本', 'float_share': '流通股本',
                    'total_mv': '总市值', 'circ_mv': '流通市值'
                })
                keep = ['代码', '日期', '换手率', '量比', 'PE', 'PE_TTM', 'PB',
                       '总市值', '流通市值', '总股本', '流通股本']
                available = [c for c in keep if c in df.columns]
                df = df[available]
            return df
        except Exception as e:
            print(f"获取 {trade_date} daily_basic 失败: {e}")
            return pd.DataFrame()
    
    def fetch_and_merge_by_date(self, trade_date: str) -> pd.DataFrame:
        """获取并合并指定日期的所有股票行情指标"""
        try:
            # 1. 基础日线
            df_daily = self.fetch_daily_by_date(trade_date)
            # 2. 每日指标 (PE/PB等)
            df_basic = self.fetch_daily_basic_by_date(trade_date)
            # 3. 复权因子
            df_adj = self.fetch_adj_factor_by_date(trade_date)
            
            if df_daily.empty:
                return pd.DataFrame()
            
            # 合并
            df = df_daily.merge(df_basic, on=['代码', '日期'], how='left')
            df = df.merge(df_adj, on=['代码', '日期'], how='left')
            
            return df
        except Exception as e:
            print(f"❌ 合并 {trade_date} 数据失败: {e}")
            return pd.DataFrame()

    def fetch_adj_factor_by_date(self, trade_date: str) -> pd.DataFrame:
        """获取全市场复权因子"""
        try:
            df = self.pro.adj_factor(trade_date=trade_date)
            if df is not None and not df.empty:
                df = df.rename(columns={'ts_code': '代码', 'trade_date': '日期', 'adj_factor': '复权因子'})
                df['代码'] = df['代码'].str[:6]
                return df[['代码', '日期', '复权因子']]
        except Exception as e:
            print(f"⚠️ 获取 {trade_date} 复权因子失败: {e}")
        return pd.DataFrame()
    
    # ==================== SQLite 存储 ====================
    
    def save_to_database(self, df: pd.DataFrame) -> int:
        """
        将数据保存到 SQLite（使用 REPLACE 实现 upsert）
        
        Returns:
            插入/更新的记录数
        """
        if df.empty:
            return 0
        
        # 确保列顺序和数据库一致
        columns = [
            '日期', '代码', '名称', '开盘', '最高', '最低', '收盘', '昨收', 
            '涨跌额', '涨跌幅', '成交量', '成交额', '换手率', '量比',
            'PE', 'PE_TTM', 'PB', '总市值', '流通市值', '总股本', '流通股本',
            '复权因子'
        ]
        
        # 添加缺失的列
        for col in columns:
            if col not in df.columns:
                df[col] = None
        
        df_to_save = df[columns]
        
        with sqlite3.connect(self.db_path) as conn:
            # 使用 REPLACE INTO 实现 upsert
            placeholders = ', '.join(['?' for _ in columns])
            cols_str = ', '.join(columns)
            sql = f'REPLACE INTO daily_data ({cols_str}) VALUES ({placeholders})'
            
            # 批量插入
            data = df_to_save.values.tolist()
            conn.executemany(sql, data)
            conn.commit()
            
            return len(data)
    
    def get_synced_dates(self) -> List[str]:
        """获取已同步的日期列表"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT DISTINCT 日期 FROM daily_data ORDER BY 日期')
            return [row[0] for row in cursor.fetchall()]
    
    def get_last_synced_date(self) -> Optional[str]:
        """获取最后同步的日期"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT MAX(日期) FROM daily_data')
            result = cursor.fetchone()
            return result[0] if result else None
    
    def get_stock_count(self) -> int:
        """获取股票数量"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT COUNT(DISTINCT 代码) FROM daily_data')
            return cursor.fetchone()[0]
    
    def get_record_count(self) -> int:
        """获取总记录数"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT COUNT(*) FROM daily_data')
            return cursor.fetchone()[0]
    
    # ==================== 主同步逻辑 ====================
    
    def sync_all_stocks(
        self, 
        days: int = 120, 
        start_date: str = None,
        end_date: str = None,
        progress_callback=None,
        force: bool = False
    ) -> bool:
        """
        同步全市场历史数据（SQLite 存储）
        
        策略：
        1. 按日期获取全市场数据
        2. 合并 daily + daily_basic
        3. 批量写入 SQLite（使用事务）
        """
        if not self._acquire_lock():
            print("⚠️ 另一个同步任务正在运行")
            if progress_callback:
                progress_callback(0, 0, "已有任务运行", "")
            return False
        
        try:
            # 计算日期范围
            today = datetime.now()
            if not end_date:
                end_date = today.strftime("%Y%m%d")
            if not start_date:
                start_date = (today - timedelta(days=days)).strftime("%Y%m%d")
            
            # 格式化日期（如果是 2024-01-01 格式转为 20240101）
            start_date = start_date.replace("-", "").replace("/", "")
            end_date = end_date.replace("-", "").replace("/", "")

            print(f"🚀 开始同步 {start_date} ~ {end_date} 间数据（SQLite 存储模式）...")
            
            # 获取交易日列表
            trading_days = self.get_trading_days(start_date, end_date)
            if not trading_days:
                print("❌ 获取交易日历失败")
                return False
            
            print(f"📅 交易日范围: {trading_days[0]} ~ {trading_days[-1]}，共 {len(trading_days)} 个交易日")
            
            # 增量模式：排除已经同步过且数据完整的日期
            if not force:
                # 检查已同步的日期中，哪些是缺失‘复权因子’的
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.execute("SELECT DISTINCT 日期 FROM daily_data WHERE 复权因子 IS NOT NULL")
                    complete_dates = {row[0] for row in cursor.fetchall()}
                
                trading_days = [d for d in trading_days if d not in complete_dates]
                
                if not trading_days:
                    print("✅ 所选范围内的交易日已全部同步且数据完整")
                    # 即使日期同步完了，也要检查并计算指标
                    self.recompute_technical_indicators()
                    return True
                
                print(f"📊 增量同步: 需要获取/补全 {len(trading_days)} 个交易日的数据")
            else:
                print("⚠️ 强制同步模式，清空数据库...")
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute('DELETE FROM daily_data')
                    conn.commit()
            
            # 同步
            total_records = 0
            self._stop_requested = False
            
            for i, trade_date in enumerate(trading_days):
                if self._stop_requested:
                    print("⏹️ 收到停止信号")
                    break
                
                # 获取并合并数据
                df = self.fetch_and_merge_by_date(trade_date)
                
                if not df.empty:
                    # 自动更新基础信息：如果发现新股票代码
                    self._check_for_new_stocks(df['代码'].unique())
                    
                    # 保存到 SQLite
                    count = self.save_to_database(df)
                    total_records += count
                    status = f"获取 {len(df)} 条，累计 {total_records} 条"
                else:
                    status = "无数据"
                
                if progress_callback:
                    progress_callback(i + 1, len(trading_days), trade_date, status)
                
                # 短暂间隔
                time.sleep(0.15)
            
            # 重新同步一次基础信息，确保名称最新
            self.sync_stock_basic()
            
            # 计算技术指标（前复权、MA、VMA）
            print("📈 正在计算技术指标（前复权、均线等）...")
            if progress_callback:
                progress_callback(len(trading_days), len(trading_days), "计算中", "计算均线指标...")
            self.recompute_technical_indicators()
            
            # 保存元数据（反映数据库全量状态）
            all_synced_dates = self.get_synced_dates()
            metadata = {
                "last_sync_date": datetime.now().isoformat(),
                "total_stocks": self.get_stock_count(),
                "total_records": self.get_record_count(),
                "total_days": len(all_synced_dates),
                "date_range": {
                    "start": all_synced_dates[0] if all_synced_dates else None,
                    "end": all_synced_dates[-1] if all_synced_dates else None
                },
                "storage": "sqlite",
                "db_file": os.path.relpath(self.db_path)
            }
            self.save_metadata(metadata)
            
            print(f"✅ 同步完成: {len(trading_days)} 个交易日, {self.get_stock_count()} 只股票, {total_records} 条记录")
            return True
        
        finally:
            self._release_lock()
    
    def _check_for_new_stocks(self, current_codes: List[str]):
        """检查是否有新股票出现，如果有则同步基础表"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("SELECT 代码 FROM stock_basic")
                synced_codes = {row[0] for row in cursor.fetchall()}
            
            new_codes = set(current_codes) - synced_codes
            if new_codes:
                print(f"📢 发现 {len(new_codes)} 只新股票记录，更新基础信息表...")
                self.sync_stock_basic()
        except:
            pass

    # ==================== 数据查询 ====================
    
    def get_stock_history(self, code: str, days: int = None) -> pd.DataFrame:
        """获取单只股票历史数据"""
        sql = '''
            SELECT d.*, b.名称 
            FROM daily_data d
            LEFT JOIN stock_basic b ON d.代码 = b.代码
            WHERE d.代码 = ? 
            ORDER BY d.日期
        '''
        params = [code]
        
        if days:
            sql = f'''
                SELECT d.*, b.名称 
                FROM daily_data d
                LEFT JOIN stock_basic b ON d.代码 = b.代码
                WHERE d.代码 = ? 
                ORDER BY d.日期 DESC 
                LIMIT ?
            '''
            params = [code, days]
        
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql_query(sql, conn, params=params)
        
        if days:
            df = df.sort_values('日期')
        
        return df

    def get_daily_data(self, trade_date: str) -> pd.DataFrame:
        """获取某一天的全市场数据"""
        sql = '''
            SELECT d.*, b.名称 
            FROM daily_data d
            LEFT JOIN stock_basic b ON d.代码 = b.代码
            WHERE d.日期 = ?
        '''
        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql_query(sql, conn, params=[trade_date])
    
    def query(self, sql: str, params: tuple = None) -> pd.DataFrame:
        """执行自定义 SQL 查询"""
        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql_query(sql, conn, params=params)
    
    def get_stocks_by_filter(
        self,
        trade_date: str = None,
        min_pct_chg: float = None,
        max_pct_chg: float = None,
        min_pe: float = None,
        max_pe: float = None,
        min_pb: float = None,
        max_pb: float = None,
        max_market_cap: float = None,  # 亿
        min_turnover: float = None,
        limit: int = 100
    ) -> pd.DataFrame:
        """
        按条件筛选股票
        
        示例：
            # 获取涨停股
            get_stocks_by_filter(trade_date='20260130', min_pct_chg=9.5)
            
            # 获取低 PE 小盘股
            get_stocks_by_filter(max_pe=20, max_market_cap=50)
        """
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
        
        if min_pb is not None:
            conditions.append('PB >= ?')
            params.append(min_pb)
        
        if max_pb is not None:
            conditions.append('PB <= ?')
            params.append(max_pb)
        
        if max_market_cap is not None:
            conditions.append('流通市值 <= ?')
            params.append(max_market_cap * 1e4)  # 亿 -> 万
        
        if min_turnover is not None:
            conditions.append('换手率 >= ?')
            params.append(min_turnover)
        
        where_clause = ' AND '.join(conditions) if conditions else '1=1'
        sql = f'''
            SELECT * FROM daily_data 
            WHERE {where_clause}
            ORDER BY 日期 DESC, 涨跌幅 DESC
            LIMIT ?
        '''
        params.append(limit)
        
        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql_query(sql, conn, params=params)
    
    def get_zt_stocks(self, trade_date: str) -> pd.DataFrame:
        """获取涨停股（涨幅 >= 9.5%）"""
        return self.get_stocks_by_filter(trade_date=trade_date, min_pct_chg=9.5, limit=500)
    
    def get_sync_status_info(self) -> dict:
        """获取同步状态信息"""
        metadata = self.get_metadata()
        
        db_size = 0
        if os.path.exists(self.db_path):
            db_size = os.path.getsize(self.db_path) / 1024 / 1024
        
        return {
            "last_sync": metadata.get("last_sync_date"),
            "total_stocks": self.get_stock_count(),
            "total_records": self.get_record_count(),
            "days": metadata.get("days", 0),
            "date_range": metadata.get("date_range", {}),
            "db_size_mb": round(db_size, 2),
            "storage": "sqlite",
            "db_file": self.db_path
        }


# 命令行入口
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="A股数据同步工具（SQLite 存储）")
    parser.add_argument("--days", type=int, default=120, help="同步天数")
    parser.add_argument("--status", action="store_true", help="查看状态")
    parser.add_argument("--force", action="store_true", help="强制全量同步")
    parser.add_argument("--query", type=str, help="执行 SQL 查询")
    
    args = parser.parse_args()
    sync = DataSyncService()
    
    if args.status:
        status = sync.get_sync_status_info()
        print("📊 同步状态:")
        print(f"  数据库: {status['db_file']}")
        print(f"  大小: {status['db_size_mb']} MB")
        print(f"  股票数量: {status['total_stocks']}")
        print(f"  记录总数: {status['total_records']}")
        print(f"  日期范围: {status['date_range']}")
        print(f"  最后同步: {status['last_sync']}")
    elif args.query:
        print(f"执行查询: {args.query}")
        df = sync.query(args.query)
        print(df.to_string())
    else:
        def progress(current, total, date, status):
            pct = current / total * 100 if total > 0 else 0
            print(f"[{current}/{total}] {pct:.0f}% | {date} | {status}")
        
        sync.sync_all_stocks(days=args.days, progress_callback=progress, force=args.force)
