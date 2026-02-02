"""
本地数据同步服务
负责拉取和管理A股历史数据（按股票代码分区存储）
"""
import os
import json
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random

# 增加全局 User-Agent 伪装
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def _patched_akshare_requests():
    """
    这是一个实验性技巧：尝试影响全局 requests 行为，
    虽然 akshare 内部自建 session，但我们可以尝试提供一个稳健的 UA 列表
    """
    USER_AGENTS = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ]
    return random.choice(USER_AGENTS)


class DataSyncService:
    """
    本地数据同步服务
    
    存储结构（按股票代码分区）：
        data/
        ├── stocks/              # 按股票代码分区
        │   ├── 000001.parquet   # 平安银行120天历史
        │   ├── 000002.parquet   # 万科A
        │   └── ...
        └── metadata.json        # 元数据
    
    使用场景：
        - AkShare 获取涨停股池（实时）
        - 从本地获取单只股票历史数据（快速）
    """
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.stocks_dir = os.path.join(data_dir, "stocks")
        self.metadata_path = os.path.join(data_dir, "metadata.json")
        self._stop_requested = False # 停止标志
        
        # 创建目录
        os.makedirs(self.stocks_dir, exist_ok=True)

    def request_stop(self):
        """请求停止同步"""
        self._stop_requested = True

    def get_metadata(self) -> dict:
        """获取元数据"""
        if os.path.exists(self.metadata_path):
            with open(self.metadata_path, 'r') as f:
                return json.load(f)
        return {
            "last_sync_date": None,
            "total_stocks": 0,
            "days": 0,
            "date_range": {},
        }
    
    def save_metadata(self, metadata: dict):
        """保存元数据"""
        with open(self.metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
    
    def get_all_stock_codes(self) -> List[str]:
        """获取所有A股股票代码"""
        try:
            df = ak.stock_info_a_code_name()
            return df['code'].tolist()
        except Exception as e:
            print(f"获取股票列表失败: {e}")
            return []
    
    def get_synced_stocks(self) -> set:
        """获取已同步的股票代码"""
        synced = set()
        if os.path.exists(self.stocks_dir):
            for f in os.listdir(self.stocks_dir):
                if f.endswith('.parquet'):
                    code = f.replace('.parquet', '')
                    synced.add(code)
        return synced
    
    def sync_single_stock(self, code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """
        同步单只股票的历史数据
        
        Args:
            code: 股票代码
            start_date: 起始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
        
        Returns:
            K线数据 DataFrame
        """
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                df = ak.stock_zh_a_hist(
                    symbol=code,
                    period="daily",
                    start_date=start_date,
                    end_date=end_date,
                    adjust="qfq"
                )
                
                if df is not None and not df.empty:
                    df['代码'] = code
                
                return df
            except Exception as e:
                if attempt < max_retries - 1:
                    # 重试前等待，逐次增加等待时间
                    time.sleep(1 + attempt * 2)
                else:
                    return None
        
        return None
    
    def check_api_health(self) -> bool:
        """检测 API 是否通畅（心跳检测）"""
        try:
            # 随机选一只权重股测试，如 000001
            df = ak.stock_zh_a_hist(
                symbol="000001",
                period="daily",
                start_date=(datetime.now() - timedelta(days=10)).strftime("%Y%m%d"),
                end_date=(datetime.now() - timedelta(days=1)).strftime("%Y%m%d"),
                adjust="qfq"
            )
            return df is not None and not df.empty
        except Exception:
            return False

    def _sync_stock_incremental(self, code: str, target_days: int, end_date: str) -> dict:
        """
        增量同步单只股票
        
        增量逻辑：
        - 检查头部：是否需要补充更早的历史数据
        - 检查尾部：是否需要追加新数据
        """
        file_path = os.path.join(self.stocks_dir, f"{code}.parquet")
        target_start = datetime.strptime(end_date, "%Y%m%d") - timedelta(days=target_days)
        target_start_str = target_start.strftime("%Y%m%d")
        
        try:
            if os.path.exists(file_path):
                existing_df = pd.read_parquet(file_path)
                if existing_df.empty:
                    df = self.sync_single_stock(code, target_start_str, end_date)
                    if df is not None and not df.empty:
                        df.to_parquet(file_path, index=False)
                        return {"status": "new", "new_records": len(df)}
                    return {"status": "failed", "new_records": 0}
                
                existing_df['日期'] = pd.to_datetime(existing_df['日期'])
                first_date = existing_df['日期'].min()
                last_date = existing_df['日期'].max()
                
                new_records = 0
                dfs_to_merge = [existing_df]
                
                # 1. 检查头部
                if first_date > target_start:
                    head_end = (first_date - timedelta(days=1)).strftime("%Y%m%d")
                    head_df = self.sync_single_stock(code, target_start_str, head_end)
                    if head_df is not None and not head_df.empty:
                        dfs_to_merge.insert(0, head_df)
                        new_records += len(head_df)
                
                # 2. 检查尾部
                if last_date.strftime("%Y%m%d") < end_date:
                    tail_start = (last_date + timedelta(days=1)).strftime("%Y%m%d")
                    tail_df = self.sync_single_stock(code, tail_start, end_date)
                    if tail_df is not None and not tail_df.empty:
                        dfs_to_merge.append(tail_df)
                        new_records += len(tail_df)
                
                if new_records > 0:
                    combined = pd.concat(dfs_to_merge, ignore_index=True)
                    combined['日期'] = pd.to_datetime(combined['日期'])
                    combined = combined.drop_duplicates(subset=['日期'], keep='last')
                    combined = combined.sort_values('日期')
                    combined.to_parquet(file_path, index=False)
                    return {"status": "updated", "new_records": new_records}
                else:
                    return {"status": "skipped", "new_records": 0}
            else:
                df = self.sync_single_stock(code, target_start_str, end_date)
                if df is not None and not df.empty:
                    df.to_parquet(file_path, index=False)
                    return {"status": "new", "new_records": len(df)}
                return {"status": "failed", "new_records": 0}
        except Exception:
            return {"status": "failed", "new_records": 0}
    
    def sync_all_stocks(
        self, 
        days: int = 120, 
        max_workers: int = 1, # 建议默认单线程，避免封禁
        progress_callback=None,
        force: bool = False
    ) -> bool:
        """
        同步所有股票的历史数据
        
        策略：
        - 默认单线程，避免高频请求触发 IP 封锁
        - 定期进行 API 健康检查
        - 触发限流后自动进入冷却，支持重试
        """
        print(f"开始同步所有股票的 {days} 天历史数据...")
        
        all_codes = self.get_all_stock_codes()
        if not all_codes:
            return False
        
        # 计算结束日期
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        end_date = yesterday.strftime("%Y%m%d")
        start_date = (yesterday - timedelta(days=days)).strftime("%Y%m%d")
        
        if force:
            for f in os.listdir(self.stocks_dir):
                if f.endswith('.parquet'):
                    os.remove(os.path.join(self.stocks_dir, f))
        
        stats = {"new": 0, "updated": 0, "skipped": 0, "failed": 0, "new_records": 0}
        cool_down_minutes = 5
        consecutive_fails = 0
        
        # 使用单线程或多线程执行
        self._stop_requested = False
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for code in all_codes:
                futures[executor.submit(self._sync_stock_incremental, code, days, end_date)] = code
            
            for i, future in enumerate(as_completed(futures)):
                # 检查中断指令
                if self._stop_requested:
                    print("接收到手动中断指令，正在停止同步...")
                    if progress_callback:
                        progress_callback(i, len(all_codes), "N/A", "手动中断中...")
                    break
                
                code = futures[future]
                
                # 每50只股票做一次健康检查
                if (i + 1) % 50 == 0:
                    if not self.check_api_health():
                        print(f"检测到 API 受限，进入冷却 {cool_down_minutes} 分钟...")
                        if progress_callback:
                            progress_callback(i + 1, len(all_codes), code, f"触发限流，进入冷却({cool_down_minutes}min)")
                        time.sleep(cool_down_minutes * 60)
                        
                        # 冷却后再次检查
                        while not self.check_api_health():
                            print("依然受限，继续等待...")
                            time.sleep(60)
                
                try:
                    result = future.result(timeout=60) # 增加超时控制
                    status = result["status"]
                    stats[status] += 1
                    stats["new_records"] += result["new_records"]
                    
                    if status == "failed":
                        consecutive_fails += 1
                    else:
                        consecutive_fails = 0
                except Exception:
                    stats["failed"] += 1
                    consecutive_fails += 1
                
                # 如果连续失败太多，很可能已经由 IP 被封，停止
                if consecutive_fails > 10:
                    print("连续失败超过10次，强制停止同步以保护 IP")
                    if progress_callback:
                        progress_callback(i + 1, len(all_codes), code, "严重限流，已停止同步")
                    return False
                
                # 进度回调
                if progress_callback:
                    status_text = "同步中" if stats["failed"] == 0 else f"正在同步 (失败:{stats['failed']})"
                    progress_callback(i + 1, len(all_codes), code, status_text)
                
                # 微延迟，避免请求频率过高
                if max_workers == 1:
                    time.sleep(0.5)
                elif (i + 1) % 10 == 0:
                    time.sleep(1)
        
        print(f"同步完成: 新增 {stats['new']}, 更新 {stats['updated']}, 跳过 {stats['skipped']}, 失败 {stats['failed']}")
        
        metadata = {
            "last_sync_date": datetime.now().isoformat(),
            "total_stocks": len(self.get_synced_stocks()),
            "days": days,
            "date_range": {"start": start_date, "end": end_date}
        }
        self.save_metadata(metadata)
        return True
    
    def get_stock_history(self, code: str) -> pd.DataFrame:
        """
        从本地获取单只股票的历史数据
        
        Args:
            code: 股票代码
        
        Returns:
            股票历史数据 DataFrame
        """
        file_path = os.path.join(self.stocks_dir, f"{code}.parquet")
        
        if os.path.exists(file_path):
            return pd.read_parquet(file_path)
        
        return pd.DataFrame()
    
    def get_stock_history_or_fetch(self, code: str, days: int = 120) -> pd.DataFrame:
        """
        获取股票历史数据，本地没有则从 AkShare 获取
        
        Args:
            code: 股票代码
            days: 天数
        
        Returns:
            股票历史数据 DataFrame
        """
        # 先尝试从本地获取
        df = self.get_stock_history(code)
        if not df.empty:
            return df
        
        # 本地没有，从 AkShare 获取
        print(f"本地无 {code} 数据，从 AkShare 获取...")
        yesterday = datetime.now() - timedelta(days=1)
        end_date = yesterday.strftime("%Y%m%d")
        start_date = (yesterday - timedelta(days=days)).strftime("%Y%m%d")
        
        df = self.sync_single_stock(code, start_date, end_date)
        
        # 保存到本地
        if df is not None and not df.empty:
            file_path = os.path.join(self.stocks_dir, f"{code}.parquet")
            df.to_parquet(file_path, index=False)
        
        return df if df is not None else pd.DataFrame()
    
    def update_stock(self, code: str) -> bool:
        """
        更新单只股票的数据（增量更新）
        
        Args:
            code: 股票代码
        
        Returns:
            是否成功
        """
        existing_df = self.get_stock_history(code)
        
        if existing_df.empty:
            # 没有历史数据，全量获取
            df = self.get_stock_history_or_fetch(code)
            return not df.empty
        
        # 获取已有数据的最后日期
        existing_df['日期'] = pd.to_datetime(existing_df['日期'])
        last_date = existing_df['日期'].max()
        
        # 从最后日期+1天开始获取
        start_date = (last_date + timedelta(days=1)).strftime("%Y%m%d")
        yesterday = datetime.now() - timedelta(days=1)
        end_date = yesterday.strftime("%Y%m%d")
        
        if start_date > end_date:
            print(f"{code} 已是最新数据")
            return True
        
        # 获取增量数据
        new_df = self.sync_single_stock(code, start_date, end_date)
        
        if new_df is not None and not new_df.empty:
            # 合并数据
            combined = pd.concat([existing_df, new_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=['日期'], keep='last')
            combined = combined.sort_values('日期')
            
            # 保存
            file_path = os.path.join(self.stocks_dir, f"{code}.parquet")
            combined.to_parquet(file_path, index=False)
            print(f"更新 {code}: 新增 {len(new_df)} 条记录")
        
        return True
    
    def get_sync_status(self) -> dict:
        """获取同步状态"""
        metadata = self.get_metadata()
        
        # 统计本地文件
        synced_stocks = self.get_synced_stocks()
        total_size = 0
        if os.path.exists(self.stocks_dir):
            for f in os.listdir(self.stocks_dir):
                if f.endswith('.parquet'):
                    total_size += os.path.getsize(os.path.join(self.stocks_dir, f))
        
        return {
            "last_sync": metadata.get("last_sync_date"),
            "total_stocks": len(synced_stocks),
            "days": metadata.get("days", 0),
            "date_range": metadata.get("date_range", {}),
            "total_size_mb": round(total_size / 1024 / 1024, 2),
        }


# 命令行入口
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="A股数据同步工具")
    parser.add_argument("--days", type=int, default=120, help="同步天数")
    parser.add_argument("--workers", type=int, default=3, help="并发数")
    parser.add_argument("--status", action="store_true", help="查看同步状态")
    parser.add_argument("--force", action="store_true", help="强制全量同步")
    
    args = parser.parse_args()
    
    sync = DataSyncService()
    
    if args.status:
        status = sync.get_sync_status()
        print("📊 同步状态:")
        print(f"  最后同步: {status['last_sync']}")
        print(f"  股票数量: {status['total_stocks']}")
        print(f"  同步天数: {status['days']}")
        print(f"  数据大小: {status['total_size_mb']} MB")
        print(f"  日期范围: {status['date_range']}")
    else:
        if args.force:
            print("⚠️ 强制全量同步模式")
        else:
            print("📊 增量同步模式（将跳过已同步的股票）")
        
        def progress(current, total, code):
            if current % 100 == 0:
                print(f"进度: {current}/{total} ({current/total*100:.1f}%)")
        
        sync.sync_all_stocks(
            days=args.days, 
            max_workers=args.workers, 
            progress_callback=progress,
            force=args.force
        )
