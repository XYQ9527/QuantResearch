"""
数据入库流水线

职责：组织五张核心表的完整拉取和入库流程
    - trade_cal:   交易日历（一次性全量，优先初始化）
    - stock_basic: 股票基础信息（一次性全量，优先初始化）
    - daily_h:     日线行情（并发拉取，耗时较长）
    - adj_factor:  复权因子（并发拉取，耗时较长）
    - daily_basic: 每日指标（并发拉取，市值/PE/PB/换手率）

使用方式：
    直接运行当前文件调试：python pipeline.py
    被其他模块调用：from pipeline import run_daily_pipeline
"""
import time
import logging
import pandas as pd

# 1. 顶部加路径修复
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 2. import从同级改为带模块路径

from datapipeline.tushare_fetcher import TushareDataFetcher  # 新

from utils.data_manager import StockDataManager  # 新

# 3. 文件名注释更新（pipeline.py → fetch_pipeline.py）

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------ #
#  一次性全量初始化（首次建库时运行）
# ------------------------------------------------------------------ #

def run_trade_cal_pipeline(start_date: str, end_date: str):
    """
    交易日历入库流程

    一次性全量拉取，无需循环，几秒完成。
    建议覆盖足够长的时间范围（如 20100101~20301231），避免频繁重跑。
    """
    fetcher = TushareDataFetcher()
    db = StockDataManager()

    logger.info(f"拉取交易日历：{start_date} ~ {end_date}")
    df = fetcher.fetch_trade_cal(start_date, end_date)
    db.save('trade_cal', df)
    logger.info("✅ 交易日历入库完成")


def run_stock_basic_pipeline():
    """
    股票基础信息入库流程

    一次性拉取全市场在市/退市/暂停三种状态，合并入库。
    建议定期重跑（如每周一次），更新摘帽/戴帽/退市状态。
    """
    fetcher = TushareDataFetcher()
    db = StockDataManager()

    logger.info("拉取股票基础信息（L/D/P 三种状态）")
    df = fetcher.fetch_stock_basic()
    db.save('stock_basic', df)
    logger.info("✅ 股票基础信息入库完成")


# ------------------------------------------------------------------ #
#  并发拉取入库（按需运行，支持增量更新）
#  三个接口完全对称：daily_h / adj_factor / daily_basic
# ------------------------------------------------------------------ #

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
        retry_results, still_failed = fetcher.batch_fetch_daily(failed, start_date, end_date)
        results.update(retry_results)
        if still_failed:
            logger.warning(f"二轮后仍失败 {len(still_failed)} 只，需人工核查: {still_failed}")

    _batch_write(results, 'daily_h', db, chunk_size)


def run_adj_factor_pipeline(start_date: str, end_date: str, chunk_size: int = 500):
    """
    复权因子完整入库流程，结构与日线完全一致
    """
    fetcher = TushareDataFetcher(rate_limit=1200)
    db = StockDataManager()
    stock_list = fetcher.get_backtest_codes()

    results, failed = fetcher.batch_fetch_adj_factor(stock_list, start_date, end_date)

    if failed:
        logger.info(f"开始二轮补漏，共 {len(failed)} 只")
        retry_results, still_failed = fetcher.batch_fetch_adj_factor(failed, start_date, end_date)
        results.update(retry_results)
        if still_failed:
            logger.warning(f"二轮后仍失败 {len(still_failed)} 只，需人工核查: {still_failed}")

    _batch_write(results, 'adj_factor', db, chunk_size)


def run_daily_basic_pipeline(start_date: str, end_date: str, chunk_size: int = 500):
    """
    每日指标完整入库流程（市值/PE/PB/换手率等）

    结构与 run_daily_pipeline / run_adj_factor_pipeline 完全对称。
    数据量和 daily_h 相当（同样是每只股票每天一条），耗时接近。

    Args 同 run_daily_pipeline
    """
    fetcher = TushareDataFetcher(max_workers=3, rate_limit=700)
    db = StockDataManager()
    stock_list = fetcher.get_backtest_codes()
    logger.info(f"股票池数量: {len(stock_list)}")

    results, failed = fetcher.batch_fetch_daily_basic(stock_list, start_date, end_date)

    if failed:
        logger.info(f"开始二轮补漏，共 {len(failed)} 只")
        retry_results, still_failed = fetcher.batch_fetch_daily_basic(failed, start_date, end_date)
        results.update(retry_results)
        if still_failed:
            logger.warning(f"二轮后仍失败 {len(still_failed)} 只，需人工核查: {still_failed}")

    _batch_write(results, 'daily_basic', db, chunk_size)


# ------------------------------------------------------------------ #
#  内部工具函数
# ------------------------------------------------------------------ #

def _batch_write(results: dict, table_name: str, db: StockDataManager,
                 chunk_size: int):
    """
    将拉取结果分批写入数据库

    Args:
        results:    {ts_code: DataFrame} 字典
        table_name: 目标表名，对应 TABLE_SCHEMAS 里的定义
        db:         StockDataManager 实例
        chunk_size: 每批股票数量
    """
    codes = list(results.keys())
    total_chunks = (len(codes) + chunk_size - 1) // chunk_size
    write_start = time.time()

    for i in range(0, len(codes), chunk_size):
        chunk_codes = codes[i:i + chunk_size]
        dfs = [results[c] for c in chunk_codes if not results[c].empty]
        chunk_df = pd.concat(dfs, ignore_index=True)
        current = i // chunk_size + 1
        chunk_start = time.time()
        logger.info(f"[{table_name}] 写入第 {current}/{total_chunks} 批，共 {len(chunk_df)} 行")
        db.save(table_name, chunk_df)
        logger.info(f"[{table_name}] 第 {current} 批耗时: {time.time() - chunk_start:.2f}s")

    logger.info(
        f"🎉 [{table_name}] 全部写入完成 | "
        f"总耗时: {time.time() - write_start:.1f}s"
    )


# ------------------------------------------------------------------ #
#  调试入口
# ------------------------------------------------------------------ #

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S',
    )

    START_DATE = '20160101'
    END_DATE = '20260302'

    t0 = time.time()

    # ── Step 1：交易日历（几秒，优先跑）────────────────────────────
    run_trade_cal_pipeline(start_date=START_DATE, end_date=END_DATE)

    # ── Step 2：股票基础信息（几秒，优先跑）────────────────────────
    run_stock_basic_pipeline()

    # ── Step 3：日线行情（约 7 分钟）───────────────────────────────
    run_daily_pipeline(start_date=START_DATE, end_date=END_DATE)

    # ── Step 4：复权因子（约 5 分钟）───────────────────────────────
    run_adj_factor_pipeline(start_date=START_DATE, end_date=END_DATE)

    # ── Step 5：每日指标（约 7 分钟）───────────────────────────────
    run_daily_basic_pipeline(start_date=START_DATE, end_date=END_DATE, chunk_size=600)

    print(f"\n✅ 全部入库完成，总耗时: {time.time() - t0:.1f}s")
