"""
Tushare 数据服务
封装 Tushare Pro 接口，提供统一的数据获取方法
可以替代 AkShare 作为数据源
"""
import os
import tushare as ts
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List
from config import TUSHARE_TOKEN


class TushareService:
    """
    Tushare Pro 数据服务
    
    使用前需要配置 Token：
    1. 环境变量: export TUSHARE_TOKEN=your_token
    2. 配置文件: config.py 中设置 TUSHARE_TOKEN
    
    Token 获取: https://tushare.pro/register
    """
    
    def __init__(self, token: str = None):
        """
        初始化 Tushare Pro API
        
        Args:
            token: Tushare Pro Token，不传则从配置/环境变量获取
        """
        self.token = token or TUSHARE_TOKEN
        if not self.token:
            raise ValueError(
                "Tushare Token 未配置！\n"
                "请通过以下方式配置：\n"
                "1. 环境变量: export TUSHARE_TOKEN=your_token\n"
                "2. 配置文件: 在 config.py 中设置 TUSHARE_TOKEN\n"
                "Token 获取: https://tushare.pro/register"
            )
        
        ts.set_token(self.token)
        self.pro = ts.pro_api()
        self._stock_info_cache = None  # 缓存股票基本信息
    
    # ==================== 基础数据 ====================
    
    def get_stock_list(self, list_status: str = "L") -> pd.DataFrame:
        """
        获取所有股票列表
        
        Args:
            list_status: 上市状态 L-上市 D-退市 P-暂停上市
        
        Returns:
            DataFrame with columns: [ts_code, symbol, name, area, industry, market, list_date]
        """
        df = self.pro.stock_basic(
            list_status=list_status,
            fields='ts_code,symbol,name,area,industry,market,list_date,delist_date'
        )
        return df
    
    def get_all_stock_codes(self) -> List[str]:
        """获取所有股票代码（6位数字格式）"""
        df = self.get_stock_list()
        return df['symbol'].tolist()
    
    def get_trade_cal(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        获取交易日历
        
        Args:
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
        """
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")
        
        df = self.pro.trade_cal(
            exchange='SSE',
            start_date=start_date,
            end_date=end_date
        )
        return df
    
    def is_trading_day(self, date: str = None) -> bool:
        """判断是否为交易日"""
        if date is None:
            date = datetime.now().strftime("%Y%m%d")
        df = self.get_trade_cal(start_date=date, end_date=date)
        if df.empty:
            return False
        return df.iloc[0]['is_open'] == 1
    
    # ==================== 行情数据 ====================
    
    def get_daily(
        self, 
        ts_code: str = None, 
        trade_date: str = None,
        start_date: str = None,
        end_date: str = None
    ) -> pd.DataFrame:
        """
        获取日线行情
        
        Args:
            ts_code: 股票代码（带后缀，如 000001.SZ）
            trade_date: 交易日期 YYYYMMDD
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            DataFrame with columns: [ts_code, trade_date, open, high, low, close, 
                                     pre_close, change, pct_chg, vol, amount]
        """
        df = self.pro.daily(
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date
        )
        return df
    
    def get_history(self, code: str, days: int = 120) -> Optional[pd.DataFrame]:
        """
        获取个股历史K线（兼容 AkShare 风格的接口）
        
        Args:
            code: 股票代码（6位数字，如 000001）
            days: 获取天数
        
        Returns:
            K线 DataFrame
        """
        # 转换为 tushare 格式
        ts_code = self._to_ts_code(code)
        
        end_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
        
        try:
            df = self.pro.daily(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date
            )
            
            if df is None or df.empty:
                return None
            
            # 转换列名以兼容现有代码
            df = df.rename(columns={
                'trade_date': '日期',
                'open': '开盘',
                'high': '最高',
                'low': '最低',
                'close': '收盘',
                'vol': '成交量',
                'amount': '成交额',
                'pct_chg': '涨跌幅'
            })
            
            # 按日期排序
            df = df.sort_values('日期')
            df['代码'] = code
            
            return df
        except Exception as e:
            print(f"获取 {code} 历史数据失败: {e}")
            return None
    
    def get_daily_basic(
        self, 
        ts_code: str = None, 
        trade_date: str = None
    ) -> pd.DataFrame:
        """
        获取每日指标（PE/PB/换手率/市值等）
        
        Returns:
            DataFrame with columns: [ts_code, trade_date, close, turnover_rate, 
                                     pe, pe_ttm, pb, ps, ps_ttm, total_share, 
                                     float_share, total_mv, circ_mv]
        """
        df = self.pro.daily_basic(
            ts_code=ts_code,
            trade_date=trade_date
        )
        return df
    
    # ==================== 实时行情（核心功能）====================
    
    def get_realtime_quotes(self, codes: List[str] = None) -> pd.DataFrame:
        """
        获取实时行情
        
        Args:
            codes: 股票代码列表，None 则获取全市场
        
        Returns:
            实时行情 DataFrame
        """
        try:
            # 使用 tushare 的实时行情接口
            if codes:
                # 批量获取指定股票
                ts_codes = [self._to_ts_code(c) for c in codes]
                df = ts.realtime_quote(ts_code=','.join(ts_codes))
            else:
                # 获取全市场实时行情
                df = ts.realtime_list()
            
            return df
        except Exception as e:
            print(f"获取实时行情失败: {e}")
            return pd.DataFrame()
    
    # ==================== 涨停板相关（选股器核心）====================
    
    def get_zt_pool(self, date: str = None) -> pd.DataFrame:
        """
        获取涨停股池（兼容 AkShare 风格）
        
        注意：tushare 的 limit_list_d 需要较高积分权限
        如果权限不够，会返回空 DataFrame
        
        Args:
            date: 日期 YYYYMMDD，默认为最近交易日
        """
        if date is None:
            date = self._get_last_trading_day()
        
        try:
            # 尝试使用 limit_list_d 接口（需要较高积分）
            df = self.pro.limit_list_d(
                trade_date=date,
                limit_type='U'  # U-涨停 D-跌停
            )
            
            if df is not None and not df.empty:
                # 转换列名以兼容现有代码
                df = df.rename(columns={
                    'ts_code': '代码',
                    'name': '名称',
                    'close': '收盘价',
                    'pct_chg': '涨跌幅',
                })
                # 提取6位代码
                df['代码'] = df['代码'].str[:6]
                return df
        except Exception as e:
            print(f"获取涨停数据失败（可能权限不足）: {e}")
        
        return pd.DataFrame()
    
    def get_limit_list(self, trade_date: str = None, limit_type: str = 'U') -> pd.DataFrame:
        """
        获取涨跌停列表
        
        Args:
            trade_date: 交易日期 YYYYMMDD
            limit_type: U-涨停 D-跌停
        """
        if trade_date is None:
            trade_date = self._get_last_trading_day()
        
        try:
            df = self.pro.limit_list_d(
                trade_date=trade_date,
                limit_type=limit_type
            )
            return df
        except Exception as e:
            print(f"获取涨跌停数据失败: {e}")
            return pd.DataFrame()
    
    # ==================== 财务数据 ====================
    
    def get_income(self, ts_code: str, period: str = None) -> pd.DataFrame:
        """获取利润表"""
        return self.pro.income(ts_code=ts_code, period=period)
    
    def get_balancesheet(self, ts_code: str, period: str = None) -> pd.DataFrame:
        """获取资产负债表"""
        return self.pro.balancesheet(ts_code=ts_code, period=period)
    
    def get_cashflow(self, ts_code: str, period: str = None) -> pd.DataFrame:
        """获取现金流量表"""
        return self.pro.cashflow(ts_code=ts_code, period=period)
    
    def get_fina_indicator(self, ts_code: str, period: str = None) -> pd.DataFrame:
        """获取财务指标"""
        return self.pro.fina_indicator(ts_code=ts_code, period=period)
    
    # ==================== 资金流向 ====================
    
    def get_moneyflow(self, ts_code: str = None, trade_date: str = None) -> pd.DataFrame:
        """获取个股资金流向"""
        return self.pro.moneyflow(ts_code=ts_code, trade_date=trade_date)
    
    def get_moneyflow_hsgt(self, trade_date: str = None) -> pd.DataFrame:
        """获取沪深港通资金流向"""
        return self.pro.moneyflow_hsgt(trade_date=trade_date)
    
    # ==================== 龙虎榜 ====================
    
    def get_top_list(self, trade_date: str = None) -> pd.DataFrame:
        """获取龙虎榜每日统计"""
        if trade_date is None:
            trade_date = self._get_last_trading_day()
        return self.pro.top_list(trade_date=trade_date)
    
    def get_top_inst(self, trade_date: str = None) -> pd.DataFrame:
        """获取龙虎榜机构交易明细"""
        if trade_date is None:
            trade_date = self._get_last_trading_day()
        return self.pro.top_inst(trade_date=trade_date)
    
    # ==================== 板块概念 ====================
    
    def get_ths_index(self, type: str = 'N') -> pd.DataFrame:
        """
        获取同花顺指数列表
        
        Args:
            type: N-概念指数 I-行业指数 S-同花顺特色指数
        """
        return self.pro.ths_index(type=type)
    
    def get_ths_member(self, ts_code: str) -> pd.DataFrame:
        """获取同花顺指数成分股"""
        return self.pro.ths_member(ts_code=ts_code)
    
    # ==================== 工具方法 ====================
    
    def _to_ts_code(self, code: str) -> str:
        """
        将6位股票代码转换为 tushare 格式（带交易所后缀）
        
        Args:
            code: 6位股票代码，如 000001
        
        Returns:
            带后缀的代码，如 000001.SZ
        """
        if '.' in code:
            return code  # 已经是完整格式
        
        # 根据代码前缀判断交易所
        if code.startswith(('6', '5', '9')):  # 上海
            return f"{code}.SH"
        elif code.startswith(('0', '3', '2', '1')):  # 深圳
            return f"{code}.SZ"
        elif code.startswith(('4', '8')):  # 北交所
            return f"{code}.BJ"
        else:
            return f"{code}.SZ"  # 默认深圳
    
    def _get_last_trading_day(self) -> str:
        """获取最近交易日"""
        today = datetime.now()
        for i in range(10):  # 最多往前找10天
            date = (today - timedelta(days=i)).strftime("%Y%m%d")
            if self.is_trading_day(date):
                return date
        return today.strftime("%Y%m%d")
    
    def check_token_valid(self) -> bool:
        """检查 Token 是否有效"""
        try:
            df = self.pro.trade_cal(
                exchange='SSE',
                start_date='20240101',
                end_date='20240101'
            )
            return df is not None and not df.empty
        except Exception:
            return False
    
    def get_api_points(self) -> dict:
        """
        获取当前 Token 的积分和权限信息
        
        注意：这需要登录 tushare.pro 网站查看
        """
        return {
            "message": "请登录 https://tushare.pro 查看您的积分和权限",
            "token_set": bool(self.token),
            "token_valid": self.check_token_valid()
        }


# 便捷函数：快速创建服务实例
def get_tushare_service(token: str = None) -> TushareService:
    """
    获取 Tushare 服务实例
    
    用法:
        from services.tushare_service import get_tushare_service
        ts_service = get_tushare_service()
        df = ts_service.get_daily(ts_code='000001.SZ')
    """
    return TushareService(token=token)


# 命令行测试入口
if __name__ == "__main__":
    print("=== Tushare 服务测试 ===")
    
    try:
        service = TushareService()
        
        print("\n[1] 检查 Token 有效性...")
        print(f"Token 有效: {service.check_token_valid()}")
        
        print("\n[2] 获取股票列表...")
        stocks = service.get_stock_list()
        print(f"共 {len(stocks)} 只股票")
        print(stocks.head())
        
        print("\n[3] 获取日线数据（平安银行）...")
        daily = service.get_history("000001", days=10)
        if daily is not None:
            print(daily.head())
        
        print("\n[4] 获取实时行情...")
        realtime = service.get_realtime_quotes(["000001", "600000"])
        if not realtime.empty:
            print(realtime.head())
        
        print("\n✅ 测试完成！")
        
    except ValueError as e:
        print(f"\n❌ 错误: {e}")
    except Exception as e:
        print(f"\n❌ 未知错误: {e}")
