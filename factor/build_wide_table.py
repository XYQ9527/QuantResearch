"""
宽表构建器：合并 + 前复权

逐只股票处理，流程：
    Step 1：读取 daily + adj_factor + daily_basic 三张 parquet
    Step 2：按 trade_date 合并（三表 inner join）
    Step 3：检查空行（理论上校核后不应该有）
    Step 4：计算前复权价格
    Step 5：写出宽表 parquet

多线程并发处理 5190 只股票
问题股票记录到 build_report.csv，不影响其他股票继续处理
"""
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config import (
    DAILY_DIR, ADJ_DIR, BASIC_DIR,WIDE_DIR, REPORT_PATH,
)

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------ #
#  字段配置
# ------------------------------------------------------------------ #

# daily_basic 只读需要的列，减少 IO
BASIC_COLS = [
    'trade_date',
    'total_mv', 'circ_mv',
    'pe_ttm', 'pb', 'ps_ttm',
    'turnover_rate', 'turnover_rate_f',
    'volume_ratio', 'dv_ttm',
    'total_share', 'float_share', 'free_share',
]

# 核心字段空值检查（pe_ttm / dv_ttm 允许 NULL，不纳入）
KEY_COLS = ['open', 'high', 'low', 'close', 'vol', 'adj_factor', 'total_mv']


# ------------------------------------------------------------------ #
#  单只股票处理
# ------------------------------------------------------------------ #

def _process_single(ts_code: str) -> dict:
    """
    对单只股票执行合并、空值检查、前复权计算

    Returns:
        处理结果字典，status = PASS / FAIL
    """
    result = {
        'ts_code': ts_code,
        'rows': 0,
        'null_rows': 0,
        'status': 'PASS',
        'note': '',
    }

    try:
        # ── Step 1：读取三张表 ────────────────────────────────────
        df_daily = pq.read_table(
            os.path.join(DAILY_DIR, f'ts_code={ts_code}')
        ).to_pandas()

        df_adj = pq.read_table(
            os.path.join(ADJ_DIR, f'ts_code={ts_code}'),
            columns=['trade_date', 'adj_factor'],
        ).to_pandas()

        df_basic = pq.read_table(
            os.path.join(BASIC_DIR, f'ts_code={ts_code}'),
            columns=BASIC_COLS,
        ).to_pandas()

        # ── Step 2：按 trade_date 三表合并 ───────────────────────
        # 校核已保证三张表日期完全对齐，inner join 不会丢行
        df = pd.merge(df_daily, df_adj, on='trade_date', how='inner')
        df = pd.merge(df, df_basic, on='trade_date', how='inner')
        df = df.sort_values('trade_date').reset_index(drop=True)
        result['rows'] = len(df)

        # ── Step 3：检查空行 ──────────────────────────────────────
        null_mask = df[KEY_COLS].isnull().any(axis=1)
        null_rows = int(null_mask.sum())
        result['null_rows'] = null_rows

        if null_rows > 0:
            result['status'] = 'FAIL'
            result['note'] = (
                f"发现 {null_rows} 行空值，"
                f"字段：{df[KEY_COLS].isnull().sum().to_dict()}"
            )
            return result

        # ── Step 4：计算前复权价格 ────────────────────────────────
        # Tushare 返回后复权因子（累计复权，随时间递增）
        # 前复权公式：前复权价 = 原始价 × (当天因子 / 最新因子)
        # 效果：最新价格不变，历史价格向下调整
        latest_factor = df['adj_factor'].iloc[-1]

        df['adj_open'] = (df['open'] * df['adj_factor'] / latest_factor).round(4)
        df['adj_high'] = (df['high'] * df['adj_factor'] / latest_factor).round(4)
        df['adj_low'] = (df['low'] * df['adj_factor'] / latest_factor).round(4)
        df['adj_close'] = (df['close'] * df['adj_factor'] / latest_factor).round(4)
        df['adj_vol'] = (df['vol'] * latest_factor / df['adj_factor']).round(4)
        # amount 成交额不需要复权

        # ── Step 5：写出宽表 ──────────────────────────────────────
        out_dir = os.path.join(WIDE_DIR, f'ts_code={ts_code}')
        os.makedirs(out_dir, exist_ok=True)
        pq.write_table(
            pa.Table.from_pandas(df=df, preserve_index=False),
            os.path.join(out_dir, 'part-0.parquet'),
        )
        result['note'] = '完成'

    except Exception as e:
        result['status'] = 'FAIL'
        result['note'] = str(e)

    return result


# ------------------------------------------------------------------ #
#  主构建入口
# ------------------------------------------------------------------ #

def build_wide(max_workers: int = 20):
    """
    并发构建全市场宽表

    Args:
        max_workers: 并发线程数，默认 20（IO 密集型，可适当调高）
    """
    t0 = time.time()

    # 扫描 daily 目录获取股票列表（validator 已清理，目录天然干净）
    all_codes = sorted([
        f.replace('ts_code=', '')
        for f in os.listdir(DAILY_DIR)
        if os.path.isdir(os.path.join(DAILY_DIR, f))
    ])
    total = len(all_codes)
    logger.info(f"开始构建宽表 | 股票数: {total} | 并发线程: {max_workers}")
    logger.info(f"输出目录: {WIDE_DIR}")
    os.makedirs(WIDE_DIR, exist_ok=True)

    # ── 并发处理 ──────────────────────────────────────────────────
    results = []
    completed = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_code = {
            executor.submit(_process_single, code): code
            for code in all_codes
        }
        for future in as_completed(future_to_code):
            completed += 1
            results.append(future.result())
            if completed % 500 == 0 or completed == total:
                elapsed = time.time() - t0
                logger.info(
                    f"进度: {completed}/{total} "
                    f"({completed / total * 100:.1f}%) | "
                    f"速率: {completed / elapsed:.0f}只/秒 | "
                    f"耗时: {elapsed:.1f}s"
                )

    # ── 汇总结果 ──────────────────────────────────────────────────
    report_df = pd.DataFrame(results)
    pass_df = report_df[report_df['status'] == 'PASS']
    fail_df = report_df[report_df['status'] == 'FAIL']

    elapsed = time.time() - t0
    total_rows = pass_df['rows'].sum()

    logger.info(f"\n{'=' * 50}")
    logger.info(f"宽表构建完成 | 总耗时: {elapsed:.1f}s")
    logger.info(f"{'=' * 50}")
    logger.info(f"✅ 成功: {len(pass_df):>6} 只 | 总行数: {total_rows:,}")
    logger.info(f"❌ 失败: {len(fail_df):>6} 只")
    logger.info(f"{'=' * 50}")

    if not fail_df.empty:
        fail_df.to_csv(REPORT_PATH, index=False, encoding='utf-8-sig')
        logger.warning(f"\n问题股票（详见 {REPORT_PATH}）：")
        for _, row in fail_df.iterrows():
            logger.warning(f"  ❌ {row['ts_code']}  {row['note']}")
    else:
        logger.info("\n🎉 所有股票构建完成，无异常")

    # ── 展示宽表字段列表 ──────────────────────────────────────────
    sample_code = pass_df['ts_code'].iloc[0]
    sample_path = os.path.join(WIDE_DIR, f'ts_code={sample_code}', 'part-0.parquet')
    sample_df = pq.read_table(sample_path).to_pandas()
    logger.info(f"\n宽表字段（共 {len(sample_df.columns)} 列）：")
    for col in sample_df.columns:
        logger.info(f"  {col}")

    return pass_df['ts_code'].tolist()


# ------------------------------------------------------------------ #
#  调试入口
# ------------------------------------------------------------------ #

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S',
    )
    build_wide(max_workers=20)
