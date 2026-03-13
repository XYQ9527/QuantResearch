"""
数据入库流水线 fetch_pipeline.py

负责组织六张核心表的完整拉取和入库流程：

    Step 1  trade_cal    交易日历        一次性全量，几秒
    Step 2  stock_basic  股票基础信息    一次性全量，几秒
    Step 3  index_daily  指数日线        一次性全量，几秒（新增）
    Step 4  daily_h      日线行情        并发拉取，约 7 分钟
    Step 5  adj_factor   复权因子        并发拉取，约 5 分钟
    Step 6  daily_basic  每日指标        并发拉取，约 7 分钟

使用方式：
    python fetch_pipeline.py         直接运行调试
    from fetch_pipeline import ...   被 main.py 调用
"""
import os
import sys
import time
import logging

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tushare_fetcher import TushareDataFetcher
from utils.data_manager import StockDataManager
from config import INDEX_LIST

logger = logging.getLogger(__name__)


# ================================================================== #
#  Step 1-3：一次性全量初始化（首次建库时运行，也可定期重跑）
# ================================================================== #

def run_trade_cal_pipeline(start_date: str, end_date: str):
    """
    交易日历入库

    一次性全量拉取，建议覆盖足够长的时间范围
    （如 20100101~20301231），避免频繁重跑。
    """
    fetcher = TushareDataFetcher()
    db = StockDataManager()

    logger.info(f"拉取交易日历：{start_date} ~ {end_date}")
    df = fetcher.fetch_trade_cal(start_date, end_date)
    db.save('trade_cal', df)
    logger.info("✅ 交易日历入库完成")


def run_stock_basic_pipeline():
    """
    股票基础信息入库

    拉取全市场在市/退市/暂停三种状态，合并入库。
    建议定期重跑（如每周一次），更新摘帽/戴帽/退市状态。
    """
    fetcher = TushareDataFetcher()
    db = StockDataManager()

    logger.info("拉取股票基础信息（L/D/P 三种状态）")
    df = fetcher.fetch_stock_basic()
    db.save('stock_basic', df)
    logger.info("✅ 股票基础信息入库完成")


def run_index_pipeline(start_date: str, end_date: str):
    """
    指数日线数据入库

    拉取五个主要指数的日线数据，数据量极小（~7000行），几秒完成。
    建议和 trade_cal / stock_basic 一起在 Step 1 最先跑。

    指数列表（来自 config.INDEX_LIST）：
        000001.SH  上证指数    市场整体情绪
        399001.SZ  深证成指    深市情绪
        000300.SH  沪深300     大盘参考基准
        000852.SH  中证1000  ★ 主基准，与持仓小市值风格匹配
        399006.SZ  创业板指    成长股风向标
    """
    fetcher = TushareDataFetcher()
    db = StockDataManager()

    logger.info(f"拉取指数日线数据：{start_date} ~ {end_date}")
    logger.info(f"指数列表：{[name for _, name in INDEX_LIST]}")

    df = fetcher.fetch_index_daily(INDEX_LIST, start_date, end_date)

    if not df.empty:
        db.save('index_daily', df)
        logger.info("✅ 指数日线入库完成")
    else:
        logger.warning("⚠️  指数数据为空，入库跳过")


# ================================================================== #
#  Step 4-6：并发拉取入库（支持增量更新）
# ================================================================== #

def run_daily_pipeline(start_date: str, end_date: str, chunk_size: int = 500):
    """
    日线行情完整入库流程

    Args:
        start_date:  拉取起始日期 'YYYYMMDD'
        end_date:    拉取结束日期 'YYYYMMDD'
        chunk_size:  分批写入每批股票数量，平衡内存和事务开销
    """
    fetcher = TushareDataFetcher()
    db = StockDataManager()
    stock_list = fetcher.get_backtest_codes()
    logger.info(f"股票池数量: {len(stock_list)}")

    results, failed = fetcher.batch_fetch_daily(stock_list, start_date, end_date)

    if failed:
        logger.info(f"开始二轮补漏，共 {len(failed)} 只")
        retry_results, still_failed = fetcher.batch_fetch_daily(
            failed, start_date, end_date
        )
        results.update(retry_results)
        if still_failed:
            logger.warning(
                f"二轮后仍失败 {len(still_failed)} 只，需人工核查: {still_failed}"
            )

    _batch_write(results, 'daily_h', db, chunk_size)


def run_adj_factor_pipeline(start_date: str, end_date: str, chunk_size: int = 500):
    """复权因子完整入库流程，结构与日线完全一致"""
    fetcher = TushareDataFetcher(rate_limit=1200)
    db = StockDataManager()
    stock_list = fetcher.get_backtest_codes()

    results, failed = fetcher.batch_fetch_adj_factor(stock_list, start_date, end_date)

    if failed:
        logger.info(f"开始二轮补漏，共 {len(failed)} 只")
        retry_results, still_failed = fetcher.batch_fetch_adj_factor(
            failed, start_date, end_date
        )
        results.update(retry_results)
        if still_failed:
            logger.warning(
                f"二轮后仍失败 {len(still_failed)} 只，需人工核查: {still_failed}"
            )

    _batch_write(results, 'adj_factor', db, chunk_size)


def run_daily_basic_pipeline(start_date: str, end_date: str, chunk_size: int = 500):
    """
    每日指标完整入库流程（市值/PE/PB/换手率等）

    数据量和 daily_h 相当，耗时接近约 7 分钟。
    """
    fetcher = TushareDataFetcher(max_workers=3, rate_limit=700)
    db = StockDataManager()
    stock_list = fetcher.get_backtest_codes()
    logger.info(f"股票池数量: {len(stock_list)}")

    results, failed = fetcher.batch_fetch_daily_basic(stock_list, start_date, end_date)

    if failed:
        logger.info(f"开始二轮补漏，共 {len(failed)} 只")
        retry_results, still_failed = fetcher.batch_fetch_daily_basic(
            failed, start_date, end_date
        )
        results.update(retry_results)
        if still_failed:
            logger.warning(
                f"二轮后仍失败 {len(still_failed)} 只，需人工核查: {still_failed}"
            )

    _batch_write(results, 'daily_basic', db, chunk_size)


# ================================================================== #
#  内部工具
# ================================================================== #

def _batch_write(
        results: dict,
        table_name: str,
        db: StockDataManager,
        chunk_size: int,
):
    """
    将拉取结果分批写入数据库

    Args:
        results:    {ts_code: DataFrame} 字典
        table_name: 目标表名
        db:         StockDataManager 实例
        chunk_size: 每批股票数量
    """
    codes = list(results.keys())
    total_chunks = (len(codes) + chunk_size - 1) // chunk_size
    write_start = time.time()

    for i in range(0, len(codes), chunk_size):
        chunk_codes = codes[i: i + chunk_size]
        dfs = [results[c] for c in chunk_codes if not results[c].empty]
        chunk_df = pd.concat(dfs, ignore_index=True)
        current = i // chunk_size + 1
        chunk_start = time.time()

        logger.info(
            f"[{table_name}] 写入第 {current}/{total_chunks} 批，"
            f"共 {len(chunk_df)} 行"
        )
        db.save(table_name, chunk_df)
        logger.info(
            f"[{table_name}] 第 {current} 批耗时: "
            f"{time.time() - chunk_start:.2f}s"
        )

    logger.info(
        f"🎉 [{table_name}] 全部写入完成 | "
        f"总耗时: {time.time() - write_start:.1f}s"
    )


# ================================================================== #
#  调试入口
# ================================================================== #

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S',
    )

    START_DATE = '20160101'
    END_DATE = '20260310'

    t0 = time.time()

    # ── Step 1：交易日历（几秒，优先跑）────────────────────────────
    run_trade_cal_pipeline(start_date='20160101', end_date='20301231')

    # ── Step 2：股票基础信息（几秒，优先跑）─────────────────────────
    run_stock_basic_pipeline()

    # ── Step 3：指数日线（几秒，优先跑）─────────────────────────────
    run_index_pipeline(start_date=START_DATE, end_date=END_DATE)

    # ── Step 4：日线行情（约 7 分钟）─────────────────────────────────
    run_daily_pipeline(start_date=START_DATE, end_date=END_DATE)

    # ── Step 5：复权因子（约 5 分钟）─────────────────────────────────
    run_adj_factor_pipeline(start_date=START_DATE, end_date=END_DATE)

    # ── Step 6：每日指标（约 7 分钟）─────────────────────────────────
    run_daily_basic_pipeline(
        start_date=START_DATE,
        end_date=END_DATE,
        chunk_size=600,
    )

    print(f"\n✅ 全部入库完成，总耗时: {time.time() - t0:.1f}s")
