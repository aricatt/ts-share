"""
缓存服务
支持历史数据持久化缓存
"""
import os
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Any
from config import CACHE_DIR, CACHE_EXPIRE_DAYS


class CacheService:
    """缓存服务"""
    
    def __init__(self, cache_dir: str = None):
        self.cache_dir = cache_dir or CACHE_DIR
        os.makedirs(self.cache_dir, exist_ok=True)
    
    def _get_cache_path(self, key: str) -> str:
        """获取缓存文件路径"""
        # 清理 key 中的特殊字符
        safe_key = key.replace("/", "_").replace("\\", "_")
        return os.path.join(self.cache_dir, f"{safe_key}.parquet")
    
    def _get_meta_path(self, key: str) -> str:
        """获取元数据文件路径"""
        safe_key = key.replace("/", "_").replace("\\", "_")
        return os.path.join(self.cache_dir, f"{safe_key}.meta.json")
    
    def get(self, key: str) -> Optional[pd.DataFrame]:
        """
        获取缓存
        
        Args:
            key: 缓存键
        
        Returns:
            缓存的 DataFrame，不存在或过期返回 None
        """
        cache_path = self._get_cache_path(key)
        meta_path = self._get_meta_path(key)
        
        if not os.path.exists(cache_path):
            return None
        
        # 检查是否过期
        if os.path.exists(meta_path):
            with open(meta_path, 'r') as f:
                meta = json.load(f)
                expire_at = datetime.fromisoformat(meta.get('expire_at', ''))
                if datetime.now() > expire_at:
                    # 已过期，删除缓存
                    self.delete(key)
                    return None
        
        try:
            return pd.read_parquet(cache_path)
        except Exception as e:
            print(f"读取缓存失败: {e}")
            return None
    
    def set(self, key: str, df: pd.DataFrame, expire_today: bool = False) -> bool:
        """
        设置缓存
        
        Args:
            key: 缓存键
            df: 要缓存的 DataFrame
            expire_today: 是否当日过期（用于当日数据）
        
        Returns:
            是否成功
        """
        try:
            cache_path = self._get_cache_path(key)
            meta_path = self._get_meta_path(key)
            
            # 保存数据
            df.to_parquet(cache_path)
            
            # 计算过期时间
            if expire_today:
                # 当日 23:59 过期
                today = datetime.now().date()
                expire_at = datetime.combine(today, datetime.max.time())
            else:
                # 永不过期（设置为 10 年后）
                expire_at = datetime.now() + timedelta(days=3650)
            
            # 保存元数据
            meta = {
                'created_at': datetime.now().isoformat(),
                'expire_at': expire_at.isoformat(),
            }
            with open(meta_path, 'w') as f:
                json.dump(meta, f)
            
            return True
        except Exception as e:
            print(f"保存缓存失败: {e}")
            return False
    
    def delete(self, key: str) -> bool:
        """删除缓存"""
        try:
            cache_path = self._get_cache_path(key)
            meta_path = self._get_meta_path(key)
            
            if os.path.exists(cache_path):
                os.remove(cache_path)
            if os.path.exists(meta_path):
                os.remove(meta_path)
            
            return True
        except Exception as e:
            print(f"删除缓存失败: {e}")
            return False
    
    def clear_all(self) -> bool:
        """清空所有缓存"""
        try:
            import shutil
            shutil.rmtree(self.cache_dir)
            os.makedirs(self.cache_dir, exist_ok=True)
            return True
        except Exception as e:
            print(f"清空缓存失败: {e}")
            return False
