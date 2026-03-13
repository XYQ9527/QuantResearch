import os
import sys

# 把项目根目录加入Python路径，确保能找到 config / utils 等模块
# datapipeline/tushare_fetcher.py → dirname一次→ datapipeline/ → dirname两次→ QuantResearch/
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import tushare as ts
import pandas as pd
import time
import logging
from typing import List, Optional, Union, Dict
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing as mp
import threading

# 路径修复后才能正确导入这两个模块
from config import TOKEN_PATH

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------ #
#  工具类：API 限速器
# ------------------------------------------------------------------ #

class MaxEffortLimiter:
    """
    滑动窗口限速器

    Tushare 按分钟限制请求数，本类在窗口内计数，
    触顶时自动休眠等待下一个窗口，保证不超限。

    Args:
        limit: 每分钟最大请求数，默认 780（留 20 个余量，上限 800）
    """

    def __init__(self, limit: int = 780):
        self.limit = limit
        self.counter = 0
        self.window_start = time.time()
        self.lock = threading.Lock()

    def gatekeeper(self):
        """每次 API 请求前调用，超限时自动阻塞等待"""
        with self.lock:
            now = time.time()
            elapsed = now - self.window_start

            if elapsed >= 60:
                self.counter = 0
                self.window_start = now

            self.counter += 1

            if self.counter >= self.limit:
                elapsed = time.time() - self.window_start
                if elapsed < 60:
                    sleep_time = 60 - elapsed + 1
                    logger.warning(
                        f"\n[权益拉满] 窗口内已发 {self.counter} 次请求，"
                        f"耗时 {elapsed:.1f}s，强制休眠 {sleep_time:.1f}s...")
                    for i in range(int(sleep_time), 0, -1):
                        sys.stdout.write(f"\r[权益保护] 剩余等待时间: {i}s ...")
                        sys.stdout.flush()
                        time.sleep(1)
                    print("\n[重新冲刺] 窗口已重置，发起新一轮进攻！")

                self.counter = 0
                self.window_start = time.time()


# ------------------------------------------------------------------ #
#  主类：数据获取器
# ------------------------------------------------------------------ #

class TushareDataFetcher:
    """
    Tushare 数据获取器（生产级并发版）

    支持两种拉取模式：

        模式A：按股票并发拉取（数据量大，走线程池）
            - daily       每日行情
            - adj_factor  复权因子
            - daily_basic 每日指标（市值/PE/PB/换手率等）

        模式B：一次性全量拉取（数据量小，直接调用）
            - trade_cal   交易日历
            - stock_basic 股票基础信息
    """

    def __init__(
            self,
            token: str = None,
            max_workers: int = None,
            rate_limit: int = 750,
    ):
        self.token = token or self._load_token()
        ts.set_token(self.token)
        self.pro = ts.pro_api()

        cpu_count = mp.cpu_count()
        self.max_workers = max_workers if max_workers else min(cpu_count * 2, 10)
        self.limiter = MaxEffortLimiter(rate_limit)
        self.stats = {'success': 0, 'failed': 0, 'total': 0, 'start_time': None}

        logger.info(f"✅ TushareDataFetcher 初始化完成 | 并发数: {self.max_workers} | CPU核心: {cpu_count}")

    # ------------------------------------------------------------------ #
    #  内部工具方法
    # ------------------------------------------------------------------ #

    def _load_token(self) -> str:
        """
        从 config/tushare_token.txt 读取 Token
        路径来自 config.py 的 TOKEN_PATH，是绝对路径，不依赖运行目录
        """
        token_path = Path(TOKEN_PATH)  # 修复：用 TOKEN_PATH 替换写死的相对路径
        if token_path.exists():
            return token_path.read_text().strip()
        raise ValueError(f"未找到 Tushare Token，请在 {TOKEN_PATH} 中配置。")

    def _reset_stats(self):
        self.stats = {'success': 0, 'failed': 0, 'total': 0, 'start_time': time.time()}

    def _print_progress(self, completed: int, total: int, failed_codes: List[str]):
        elapsed = time.time() - self.stats['start_time']
        rate = completed / elapsed if elapsed > 0 else 0
        eta = (total - completed) / rate if rate > 0 else 0
        logger.info(
            f"进度: {completed}/{total} ({completed / total * 100:.1f}%) | "
            f"成功: {self.stats['success']} | 失败: {self.stats['failed']} | "
            f"速率: {rate:.1f}只/秒 | 预计剩余: {eta:.0f}秒"
        )
        if failed_codes and len(failed_codes) <= 5:
            logger.warning(f"失败股票: {failed_codes}")

    # ------------------------------------------------------------------ #
    #  底层单只股票获取
    # ------------------------------------------------------------------ #

    _DAILY_BASIC_FIELDS = (
        'ts_code,trade_date,'
        'total_mv,circ_mv,'
        'pe_ttm,pb,ps_ttm,'
        'turnover_rate,turnover_rate_f,'
        'volume_ratio,dv_ttm,'
        'total_share,float_share,free_share'
    )

    def _fetch_single_stock(
            self,
            ts_code: str,
            start_date: str,
            end_date: str,
            data_type: str = 'daily',
    ) -> Optional[pd.DataFrame]:
        self.limiter.gatekeeper()
        try:
            if data_type == 'daily':
                df = self.pro.daily(
                    ts_code=ts_code, start_date=start_date, end_date=end_date,
                )
            elif data_type == 'adj_factor':
                df = self.pro.adj_factor(
                    ts_code=ts_code, start_date=start_date, end_date=end_date,
                )
            elif data_type == 'daily_basic':
                df = self.pro.daily_basic(
                    ts_code=ts_code, start_date=start_date, end_date=end_date,
                    fields=self._DAILY_BASIC_FIELDS,
                )
            else:
                raise ValueError(f"不支持的数据类型: {data_type}")

            if df is None or df.empty:
                logger.debug(f"{ts_code} [{data_type}] 无数据")
                return None

            if data_type == 'daily':
                df = self._clean_daily_data(df)

            return df

        except Exception as e:
            err_msg = str(e)
            if "最多访问" in err_msg:
                logger.warning(f"⚠️ {ts_code} 触发 API 限频，交由外部二轮补漏")
            else:
                logger.error(f"❌ {ts_code} [{data_type}] 获取异常: {err_msg}")
            return None

    # ------------------------------------------------------------------ #
    #  核心并发引擎
    # ------------------------------------------------------------------ #

    def _fetch_with_thread_pool(
            self,
            ts_codes: List[str],
            start_date: str,
            end_date: str,
            data_type: str,
    ) -> tuple[Dict[str, pd.DataFrame], List[str]]:
        results: Dict[str, pd.DataFrame] = {}
        failed_codes: List[str] = []
        completed = 0
        total = len(ts_codes)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_code = {
                executor.submit(
                    self._fetch_single_stock, code, start_date, end_date, data_type
                ): code
                for code in ts_codes
            }
            for future in as_completed(future_to_code):
                code = future_to_code[future]
                completed += 1
                try:
                    df = future.result()
                    if df is not None and not df.empty:
                        results[code] = df
                        self.stats['success'] += 1
                    else:
                        failed_codes.append(code)
                        self.stats['failed'] += 1
                except Exception as e:
                    logger.error(f"{code} 线程异常: {e}")
                    failed_codes.append(code)
                    self.stats['failed'] += 1

                if completed % 50 == 0 or completed == total:
                    self._print_progress(completed, total, failed_codes)

        elapsed = time.time() - self.stats['start_time']
        logger.info(
            f"✅ [{data_type}] 批量获取完成 | "
            f"成功: {self.stats['success']} | 失败: {self.stats['failed']} | "
            f"总耗时: {elapsed:.1f}s"
        )
        if failed_codes:
            logger.warning(f"⚠️ 失败股票 ({len(failed_codes)} 只): {failed_codes}")

        return results, failed_codes

    # ------------------------------------------------------------------ #
    #  公共接口：模式A
    # ------------------------------------------------------------------ #

    def batch_fetch_daily(
            self, ts_codes: Union[str, List[str]],
            start_date: str = None, end_date: str = None,
    ) -> tuple[Dict[str, pd.DataFrame], List[str]]:
        if isinstance(ts_codes, str):
            ts_codes = [ts_codes]
        end_date = end_date or datetime.now().strftime('%Y%m%d')
        start_date = start_date or (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
        self._reset_stats()
        self.stats['total'] = len(ts_codes)
        logger.info(f"🚀 批量获取每日行情 | 股票数: {len(ts_codes)} | 区间: {start_date}~{end_date}")
        return self._fetch_with_thread_pool(ts_codes, start_date, end_date, data_type='daily')

    def batch_fetch_adj_factor(
            self, ts_codes: Union[str, List[str]],
            start_date: str = None, end_date: str = None,
    ) -> tuple[Dict[str, pd.DataFrame], List[str]]:
        if isinstance(ts_codes, str):
            ts_codes = [ts_codes]
        end_date = end_date or datetime.now().strftime('%Y%m%d')
        start_date = start_date or (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
        self._reset_stats()
        self.stats['total'] = len(ts_codes)
        logger.info(f"🚀 批量获取复权因子 | 股票数: {len(ts_codes)} | 区间: {start_date}~{end_date}")
        return self._fetch_with_thread_pool(ts_codes, start_date, end_date, data_type='adj_factor')

    def batch_fetch_daily_basic(
            self, ts_codes: Union[str, List[str]],
            start_date: str = None, end_date: str = None,
    ) -> tuple[Dict[str, pd.DataFrame], List[str]]:
        if isinstance(ts_codes, str):
            ts_codes = [ts_codes]
        end_date = end_date or datetime.now().strftime('%Y%m%d')
        start_date = start_date or (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
        self._reset_stats()
        self.stats['total'] = len(ts_codes)
        logger.info(f"🚀 批量获取每日指标 | 股票数: {len(ts_codes)} | 区间: {start_date}~{end_date}")
        return self._fetch_with_thread_pool(ts_codes, start_date, end_date, data_type='daily_basic')

    # ------------------------------------------------------------------ #
    #  公共接口：模式B
    # ------------------------------------------------------------------ #

    def fetch_trade_cal(
            self, start_date: str, end_date: str, exchange: str = 'SSE',
    ) -> pd.DataFrame:
        self.limiter.gatekeeper()
        try:
            df = self.pro.trade_cal(
                exchange=exchange, start_date=start_date, end_date=end_date,
                fields='exchange,cal_date,is_open,pretrade_date',
            )
            logger.info(f"✅ [trade_cal] 拉取完成，共 {len(df)} 条")
            return df
        except Exception as e:
            logger.error(f"❌ [trade_cal] 拉取失败: {e}")
            return pd.DataFrame()

    def fetch_stock_basic(self) -> pd.DataFrame:
        self.limiter.gatekeeper()
        try:
            df = self.pro.stock_basic(
                list_status='L', exchange='SSE,SZSE',
                fields='ts_code,name,market,exchange,industry,area,'
                       'list_date,delist_date,list_status',
            )
            logger.info(f"✅ [stock_basic] 拉取完成，共 {len(df)} 只在市股票")
            return df if df is not None else pd.DataFrame()
        except Exception as e:
            logger.error(f"❌ [stock_basic] 拉取失败: {e}")
            return pd.DataFrame()

    # ------------------------------------------------------------------ #
    #  数据清洗
    # ------------------------------------------------------------------ #

    def _clean_daily_data(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        df = df.dropna(how='all')
        df = df[df['close'] > 0]
        df = df.sort_values('trade_date', ascending=True).reset_index(drop=True)
        return df

    # ------------------------------------------------------------------ #
    #  股票池筛选
    # ------------------------------------------------------------------ #

    def get_backtest_codes(self) -> List[str]:
        df = self.pro.stock_basic(
            list_status='L', exchange='SSE,SZSE', fields='ts_code',
        )
        return df['ts_code'].tolist()


# ------------------------------------------------------------------ #
#  调试入口
# ------------------------------------------------------------------ #

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
    )

    fetcher = TushareDataFetcher()

    test_codes = ['600000.SH', '000001.SZ', '600519.SH', '000858.SZ', '601318.SH']
    results, failed = fetcher.batch_fetch_daily_basic(
        ts_codes=test_codes,
        start_date='20240101',
        end_date='20240110',
    )

    print(f"\n成功: {len(results)} 只，失败: {failed}")
    for code, df in list(results.items())[:2]:
        print(f"\n{code}:")
        print(df.head(3))
        print("字段:", df.columns.tolist())
