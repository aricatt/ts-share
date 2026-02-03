#!/usr/bin/env python3
"""
远程数据同步脚本 - 独立版
==========================

功能：
- 低频同步全A股历史K线数据（避免限流）
- 断点续传（中断后可继续）
- 增量更新（只下载新数据）
- 打包导出（方便下载到本地）

使用方法：
---------
# 1. 安装依赖
pip install akshare pandas pyarrow tqdm

# 2. 首次全量同步（低频模式）
python remote_sync.py --mode full --days 120 --delay 3

# 3. 增量更新（日常使用）
python remote_sync.py --mode incremental

# 4. 打包数据（下载到本地）
python remote_sync.py --mode export

# 5. 查看状态
python remote_sync.py --mode status

作者: TS-Share
"""

import os
import sys
import json
import time
import random
import argparse
import subprocess
from datetime import datetime, timedelta
from typing import Optional, Set, List

import akshare as ak
import pandas as pd
from tqdm import tqdm


# ============ 配置 ============
class Config:
    # 数据目录
    DATA_DIR = "data"
    STOCKS_DIR = os.path.join(DATA_DIR, "stocks")
    METADATA_FILE = os.path.join(DATA_DIR, "metadata.json")
    PROGRESS_FILE = os.path.join(DATA_DIR, "sync_progress.json")
    
    # 默认同步天数
    DEFAULT_DAYS = 120
    
    # 请求延迟（秒）- 低频模式关键参数
    DEFAULT_DELAY = 3.0      # 每次请求间隔
    DELAY_JITTER = 1.0       # 随机抖动范围
    
    # 限流保护
    MAX_CONSECUTIVE_FAILS = 5    # 连续失败次数上限
    COOLDOWN_MINUTES = 10        # 触发限流后冷却时间
    HEALTH_CHECK_INTERVAL = 100  # 每N只股票检查一次API健康
    
    # 导出配置
    EXPORT_DIR = "export"


# ============ 工具函数 ============
def log(msg: str, level: str = "INFO"):
    """打印日志"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {msg}")


def random_delay(base: float, jitter: float = 1.0):
    """随机延迟"""
    delay = base + random.uniform(0, jitter)
    time.sleep(delay)


def ensure_dirs():
    """确保目录存在"""
    os.makedirs(Config.STOCKS_DIR, exist_ok=True)
    os.makedirs(Config.EXPORT_DIR, exist_ok=True)


# ============ 元数据管理 ============
def load_metadata() -> dict:
    """加载元数据"""
    if os.path.exists(Config.METADATA_FILE):
        with open(Config.METADATA_FILE, 'r') as f:
            return json.load(f)
    return {
        "last_sync_date": None,
        "total_stocks": 0,
        "days": 0,
        "date_range": {},
    }


def save_metadata(metadata: dict):
    """保存元数据"""
    with open(Config.METADATA_FILE, 'w') as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)


def load_progress() -> dict:
    """加载同步进度（用于断点续传）"""
    if os.path.exists(Config.PROGRESS_FILE):
        with open(Config.PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return {
        "completed_codes": [],
        "failed_codes": [],
        "last_code": None,
        "start_time": None,
    }


def save_progress(progress: dict):
    """保存同步进度"""
    with open(Config.PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2, ensure_ascii=False)


def clear_progress():
    """清除进度文件"""
    if os.path.exists(Config.PROGRESS_FILE):
        os.remove(Config.PROGRESS_FILE)


# ============ API 函数 ============
def get_all_stock_codes() -> List[str]:
    """获取所有A股股票代码"""
    log("正在获取股票列表...")
    try:
        df = ak.stock_info_a_code_name()
        codes = df['code'].tolist()
        log(f"获取到 {len(codes)} 只股票")
        return codes
    except Exception as e:
        log(f"获取股票列表失败: {e}", "ERROR")
        return []


def check_api_health() -> bool:
    """检查 API 是否健康"""
    try:
        end_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=5)).strftime("%Y%m%d")
        df = ak.stock_zh_a_hist(
            symbol="000001",
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="qfq"
        )
        return df is not None and not df.empty
    except Exception:
        return False


def fetch_stock_history(code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
    """获取单只股票的历史数据"""
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
        return None


# ============ 同步逻辑 ============
def get_synced_stocks() -> Set[str]:
    """获取已同步的股票代码"""
    synced = set()
    if os.path.exists(Config.STOCKS_DIR):
        for f in os.listdir(Config.STOCKS_DIR):
            if f.endswith('.parquet'):
                code = f.replace('.parquet', '')
                synced.add(code)
    return synced


def sync_single_stock(code: str, start_date: str, end_date: str) -> dict:
    """
    同步单只股票（支持增量）
    
    返回:
        {"status": "new"|"updated"|"skipped"|"failed", "records": 数量}
    """
    file_path = os.path.join(Config.STOCKS_DIR, f"{code}.parquet")
    
    try:
        if os.path.exists(file_path):
            # 已有数据，增量更新
            existing_df = pd.read_parquet(file_path)
            if existing_df.empty:
                # 空文件，全量获取
                df = fetch_stock_history(code, start_date, end_date)
                if df is not None and not df.empty:
                    df.to_parquet(file_path, index=False)
                    return {"status": "new", "records": len(df)}
                return {"status": "failed", "records": 0}
            
            existing_df['日期'] = pd.to_datetime(existing_df['日期'])
            last_date = existing_df['日期'].max()
            
            # 检查是否需要更新
            last_date_str = last_date.strftime("%Y%m%d")
            if last_date_str >= end_date:
                return {"status": "skipped", "records": 0}
            
            # 获取增量数据
            incr_start = (last_date + timedelta(days=1)).strftime("%Y%m%d")
            new_df = fetch_stock_history(code, incr_start, end_date)
            
            if new_df is not None and not new_df.empty:
                # 合并数据
                combined = pd.concat([existing_df, new_df], ignore_index=True)
                combined['日期'] = pd.to_datetime(combined['日期'])
                combined = combined.drop_duplicates(subset=['日期'], keep='last')
                combined = combined.sort_values('日期')
                combined.to_parquet(file_path, index=False)
                return {"status": "updated", "records": len(new_df)}
            
            return {"status": "skipped", "records": 0}
        else:
            # 新股票，全量获取
            df = fetch_stock_history(code, start_date, end_date)
            if df is not None and not df.empty:
                df.to_parquet(file_path, index=False)
                return {"status": "new", "records": len(df)}
            return {"status": "failed", "records": 0}
    
    except Exception as e:
        return {"status": "failed", "records": 0}


def run_full_sync(days: int, delay: float, resume: bool = True):
    """
    全量同步
    
    Args:
        days: 同步天数
        delay: 请求间隔（秒）
        resume: 是否断点续传
    """
    ensure_dirs()
    
    log(f"🚀 开始全量同步 (天数={days}, 延迟={delay}秒)")
    
    # 获取所有股票代码
    all_codes = get_all_stock_codes()
    if not all_codes:
        log("无法获取股票列表，退出", "ERROR")
        return
    
    # 计算日期范围
    yesterday = datetime.now() - timedelta(days=1)
    end_date = yesterday.strftime("%Y%m%d")
    start_date = (yesterday - timedelta(days=days)).strftime("%Y%m%d")
    
    log(f"📅 数据范围: {start_date} ~ {end_date}")
    
    # 加载进度（断点续传）
    progress = load_progress() if resume else {"completed_codes": [], "failed_codes": []}
    completed_set = set(progress.get("completed_codes", []))
    
    if resume and completed_set:
        log(f"📌 断点续传: 已完成 {len(completed_set)} 只股票")
    
    # 过滤待同步的股票
    pending_codes = [c for c in all_codes if c not in completed_set]
    log(f"📊 待同步: {len(pending_codes)} 只股票")
    
    # 统计
    stats = {"new": 0, "updated": 0, "skipped": 0, "failed": 0, "records": 0}
    consecutive_fails = 0
    
    # 记录开始时间
    if not progress.get("start_time"):
        progress["start_time"] = datetime.now().isoformat()
    
    # 开始同步
    try:
        for i, code in enumerate(tqdm(pending_codes, desc="同步进度")):
            # API 健康检查
            if (i + 1) % Config.HEALTH_CHECK_INTERVAL == 0:
                log(f"🔍 进行 API 健康检查...")
                if not check_api_health():
                    log(f"⚠️ API 受限，进入冷却 {Config.COOLDOWN_MINUTES} 分钟", "WARN")
                    time.sleep(Config.COOLDOWN_MINUTES * 60)
            
            # 同步单只股票
            result = sync_single_stock(code, start_date, end_date)
            status = result["status"]
            stats[status] += 1
            stats["records"] += result["records"]
            
            # 更新进度
            if status != "failed":
                progress["completed_codes"].append(code)
                consecutive_fails = 0
            else:
                progress["failed_codes"].append(code)
                consecutive_fails += 1
            
            progress["last_code"] = code
            
            # 连续失败保护
            if consecutive_fails >= Config.MAX_CONSECUTIVE_FAILS:
                log(f"🛑 连续失败 {consecutive_fails} 次，触发限流保护", "WARN")
                log(f"⏳ 冷却 {Config.COOLDOWN_MINUTES} 分钟后继续...")
                save_progress(progress)
                time.sleep(Config.COOLDOWN_MINUTES * 60)
                consecutive_fails = 0
                
                # 冷却后检查 API
                if not check_api_health():
                    log("🔴 API 仍然受限，建议稍后重试", "ERROR")
                    break
            
            # 定期保存进度
            if (i + 1) % 50 == 0:
                save_progress(progress)
            
            # 随机延迟
            random_delay(delay, Config.DELAY_JITTER)
    
    except KeyboardInterrupt:
        log("\n⚠️ 用户中断，保存进度...", "WARN")
    
    finally:
        save_progress(progress)
    
    # 打印统计
    log("=" * 50)
    log(f"✅ 同步完成统计:")
    log(f"   新增: {stats['new']}")
    log(f"   更新: {stats['updated']}")
    log(f"   跳过: {stats['skipped']}")
    log(f"   失败: {stats['failed']}")
    log(f"   总记录: {stats['records']}")
    log("=" * 50)
    
    # 更新元数据
    metadata = {
        "last_sync_date": datetime.now().isoformat(),
        "total_stocks": len(get_synced_stocks()),
        "days": days,
        "date_range": {"start": start_date, "end": end_date},
    }
    save_metadata(metadata)
    
    # 同步完成，清除进度文件
    if stats["failed"] == 0:
        clear_progress()
        log("🎉 全量同步成功完成！")


def run_incremental_sync(delay: float = 2.0):
    """增量同步（只更新最新数据）"""
    ensure_dirs()
    
    log("📊 开始增量同步...")
    
    # 获取已同步的股票
    synced_codes = get_synced_stocks()
    if not synced_codes:
        log("没有已同步的数据，请先运行全量同步", "WARN")
        return
    
    log(f"📌 已有 {len(synced_codes)} 只股票")
    
    # 计算日期
    yesterday = datetime.now() - timedelta(days=1)
    end_date = yesterday.strftime("%Y%m%d")
    
    stats = {"updated": 0, "skipped": 0, "failed": 0, "records": 0}
    
    for code in tqdm(synced_codes, desc="增量更新"):
        # 读取现有数据，确定增量起点
        file_path = os.path.join(Config.STOCKS_DIR, f"{code}.parquet")
        try:
            existing_df = pd.read_parquet(file_path)
            existing_df['日期'] = pd.to_datetime(existing_df['日期'])
            last_date = existing_df['日期'].max()
            start_date = (last_date + timedelta(days=1)).strftime("%Y%m%d")
            
            if start_date > end_date:
                stats["skipped"] += 1
                continue
            
            result = sync_single_stock(code, start_date, end_date)
            stats[result["status"]] = stats.get(result["status"], 0) + 1
            stats["records"] += result["records"]
            
        except Exception as e:
            stats["failed"] += 1
        
        random_delay(delay, 0.5)
    
    log("=" * 50)
    log(f"✅ 增量同步完成:")
    log(f"   更新: {stats.get('updated', 0)}")
    log(f"   跳过: {stats['skipped']}")
    log(f"   失败: {stats['failed']}")
    log(f"   新增记录: {stats['records']}")


def run_export():
    """打包导出数据"""
    ensure_dirs()
    
    if not os.path.exists(Config.STOCKS_DIR):
        log("没有可导出的数据", "WARN")
        return
    
    # 生成打包文件名
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_name = f"ts_share_data_{timestamp}"
    archive_path = os.path.join(Config.EXPORT_DIR, archive_name)
    
    log(f"📦 正在打包数据到 {archive_path}.tar.gz ...")
    
    try:
        # 使用 tar 打包压缩
        subprocess.run([
            "tar", "-czvf", 
            f"{archive_path}.tar.gz",
            "-C", Config.DATA_DIR,
            "stocks",
            "metadata.json"
        ], check=True, capture_output=True)
        
        # 获取文件大小
        size_mb = os.path.getsize(f"{archive_path}.tar.gz") / (1024 * 1024)
        log(f"✅ 打包完成: {archive_path}.tar.gz ({size_mb:.2f} MB)")
        log(f"💡 下载命令: scp user@server:{os.path.abspath(archive_path)}.tar.gz ./")
        
    except Exception as e:
        log(f"打包失败: {e}", "ERROR")


def show_status():
    """显示同步状态"""
    ensure_dirs()
    
    print("=" * 50)
    print("📊 TS-Share 数据同步状态")
    print("=" * 50)
    
    # 元数据
    metadata = load_metadata()
    synced_count = len(get_synced_stocks())
    
    print(f"\n📌 已同步股票: {synced_count}")
    print(f"📅 历史天数: {metadata.get('days', 'N/A')}")
    print(f"🕐 最后同步: {metadata.get('last_sync_date', 'N/A')}")
    
    date_range = metadata.get('date_range', {})
    if date_range:
        print(f"📆 数据范围: {date_range.get('start', '?')} ~ {date_range.get('end', '?')}")
    
    # 数据大小
    total_size = 0
    if os.path.exists(Config.STOCKS_DIR):
        for f in os.listdir(Config.STOCKS_DIR):
            if f.endswith('.parquet'):
                total_size += os.path.getsize(os.path.join(Config.STOCKS_DIR, f))
    print(f"💾 数据大小: {total_size / (1024 * 1024):.2f} MB")
    
    # 进度信息
    progress = load_progress()
    if progress.get("completed_codes"):
        print(f"\n⏳ 同步进度:")
        print(f"   已完成: {len(progress['completed_codes'])}")
        print(f"   失败: {len(progress.get('failed_codes', []))}")
        print(f"   开始时间: {progress.get('start_time', 'N/A')}")
    
    print("=" * 50)


# ============ 主入口 ============
def main():
    parser = argparse.ArgumentParser(
        description="TS-Share 远程数据同步脚本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python remote_sync.py --mode full --days 120 --delay 3
  python remote_sync.py --mode incremental
  python remote_sync.py --mode export
  python remote_sync.py --mode status
        """
    )
    
    parser.add_argument(
        "--mode", 
        choices=["full", "incremental", "export", "status"],
        default="status",
        help="运行模式: full=全量同步, incremental=增量更新, export=打包导出, status=查看状态"
    )
    
    parser.add_argument(
        "--days",
        type=int,
        default=Config.DEFAULT_DAYS,
        help=f"同步天数 (默认: {Config.DEFAULT_DAYS})"
    )
    
    parser.add_argument(
        "--delay",
        type=float,
        default=Config.DEFAULT_DELAY,
        help=f"请求间隔秒数 (默认: {Config.DEFAULT_DELAY})"
    )
    
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="禁用断点续传，从头开始"
    )
    
    args = parser.parse_args()
    
    if args.mode == "full":
        run_full_sync(
            days=args.days, 
            delay=args.delay, 
            resume=not args.no_resume
        )
    elif args.mode == "incremental":
        run_incremental_sync(delay=args.delay)
    elif args.mode == "export":
        run_export()
    elif args.mode == "status":
        show_status()


if __name__ == "__main__":
    main()
