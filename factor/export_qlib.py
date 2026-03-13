"""
Qlib 数据转换模块 parquet_to_qlibdata.py

从最终信号层 step3_signals_table 读取所有字段，同步到 Qlib .day.bin 格式
采用排除法：除了少数无用字段，其余全部同步

排除字段：
    trade_date      日期列，不是特征
    adj_factor      复权因子，指标已算完不再需要
    （open/high/low/close/vol 在 factors_compute 层已排除，signals 层不存在）

同步字段（signals 全部列 去掉上述2列）：
    前复权价格  adj_open/high/low/close/vol/amount
    基本面      total_mv/circ_mv/pe_ttm/pb/ps_ttm/dv_ttm
    换手率      turnover_rate/turnover_rate_f/volume_ratio
    股本        total_share/float_share/free_share
    技术指标    macd_dif/dea/bar / kdj_k/d/j / rsi_6/12 / boll_upper/mid/lower
               mom_5/10/20 / ma_5/10/20/60 / atr_14
    形态信号    kdj_cross / rsi_new_low / macd_shrink
    factor      固定写 1.0（已前复权）

signal_builder 新增任何字段，本脚本无需改动，自动同步。
股票代码格式：000001.SZ → sz000001
"""
import os
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sqlite3
import struct
import time
import logging
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import (
    DB_PATH, SIGNALS_DIR,
    QLIB_DIR, QLIB_CAL, QLIB_INST, QLIB_FEAT,
)

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------ #
#  排除字段（其余全部同步）
# ------------------------------------------------------------------ #

EXCLUDE_COLS = {
    'trade_date',   # 日期列，不是特征
    'adj_factor',   # 复权因子，指标已算完不再需要
    # open/high/low/close/vol 在 factors_compute 层已排除，signals 层不存在
}


# ------------------------------------------------------------------ #
#  工具函数
# ------------------------------------------------------------------ #

def _code_to_qlib(ts_code: str) -> str:
    """000001.SZ → sz000001"""
    code, exchange = ts_code.split(".")
    return f"{exchange.lower()}{code}"


def _write_bin(values: np.ndarray, start_index: int, out_path: str):
    """写入 Qlib .day.bin 格式"""
    with open(out_path, 'wb') as f:
        f.write(struct.pack('<I', start_index))
        f.write(values.astype('<f4').tobytes())


# ------------------------------------------------------------------ #
#  Step 1：生成 calendars/day.txt
# ------------------------------------------------------------------ #

def dump_calendars() -> list:
    logger.info("Step 1：生成 calendars/day.txt ...")
    conn: sqlite3.Connection = sqlite3.connect(DB_PATH)
    df = pd.read_sql(
        "SELECT cal_date FROM trade_cal WHERE is_open=1 ORDER BY cal_date",
        conn,
    )
    conn.close()

    df['cal_date'] = pd.to_datetime(
        df['cal_date'], format='%Y%m%d'
    ).dt.strftime('%Y-%m-%d')

    os.makedirs(QLIB_CAL, exist_ok=True)
    cal_path = os.path.join(QLIB_CAL, "day.txt")
    df['cal_date'].to_csv(cal_path, index=False, header=False)

    logger.info(f"  ✅ 交易日数：{len(df)} 天 → {cal_path}")
    return df['cal_date'].tolist()


# ------------------------------------------------------------------ #
#  Step 2：生成 instruments/all.txt
# ------------------------------------------------------------------ #

def dump_instruments():
    logger.info("Step 2：生成 instruments/all.txt ...")
    conn: sqlite3.Connection = sqlite3.connect(DB_PATH)
    df = pd.read_sql(
        "SELECT ts_code, list_date, delist_date FROM stock_basic",
        conn,
    )
    conn.close()

    df['qlib_code']   = df['ts_code'].apply(_code_to_qlib)
    df['list_date']   = pd.to_datetime(
        df['list_date'], format='%Y%m%d'
    ).dt.strftime('%Y-%m-%d')
    df['delist_date'] = (
        df['delist_date'].replace('', None).fillna('20991231')
    )
    df['delist_date'] = pd.to_datetime(
        df['delist_date'], format='%Y%m%d'
    ).dt.strftime('%Y-%m-%d')

    os.makedirs(QLIB_INST, exist_ok=True)
    inst_path = os.path.join(QLIB_INST, "all.txt")
    with open(inst_path, 'w') as f:
        for _, row in df.iterrows():
            f.write(
                f"{row['qlib_code']}\t{row['list_date']}\t{row['delist_date']}\n"
            )

    logger.info(f"  ✅ 股票数：{len(df)} 只 → {inst_path}")


# ------------------------------------------------------------------ #
#  Step 3：生成 features（单只股票）
# ------------------------------------------------------------------ #

def _dump_single(ts_code: str, cal_index: dict, sync_fields: list) -> dict:
    result = {'ts_code': ts_code, 'status': 'PASS', 'note': ''}

    try:
        # ── 读 signals 最终宽表 ───────────────────────────────────
        signal_path = os.path.join(SIGNALS_DIR, f'ts_code={ts_code}')
        df = pq.read_table(signal_path).to_pandas()

        if df.empty:
            result['status'] = 'SKIP'
            result['note']   = 'signals表为空'
            return result

        # ── 日期排序 ──────────────────────────────────────────────
        df['trade_date'] = pd.to_datetime(
            df['trade_date']
        ).dt.strftime('%Y-%m-%d')
        df = df.sort_values('trade_date').reset_index(drop=True)

        # ── 找起始日期索引 ────────────────────────────────────────
        first_date = df['trade_date'].iloc[0]
        if first_date not in cal_index:
            result['status'] = 'FAIL'
            result['note']   = f"起始日期 {first_date} 不在 calendar 中"
            return result

        start_index = cal_index[first_date]

        # ── 创建输出目录 ──────────────────────────────────────────
        qlib_code = _code_to_qlib(ts_code)
        feat_dir  = os.path.join(QLIB_FEAT, qlib_code)
        os.makedirs(feat_dir, exist_ok=True)

        # ── 写入所有字段 ──────────────────────────────────────────
        for col in sync_fields:
            if col in df.columns:
                values   = df[col].values.astype(np.float32)
                out_path = os.path.join(feat_dir, f"{col}.day.bin")
                _write_bin(values, start_index, out_path)

        # ── factor 固定写 1.0（已前复权）─────────────────────────
        _write_bin(
            np.ones(len(df), dtype=np.float32),
            start_index,
            os.path.join(feat_dir, "factor.day.bin"),
        )

        result['note'] = f"{len(df)}天 / {len(sync_fields) + 1}字段"

    except Exception as e:
        result['status'] = 'FAIL'
        result['note']   = str(e)

    return result


# ------------------------------------------------------------------ #
#  Step 3 主函数
# ------------------------------------------------------------------ #

def dump_features(calendar: list, max_workers: int = 20):
    logger.info("Step 3：生成 features ...")

    # 从第一只股票读取字段列表（排除法）
    all_codes = sorted([
        f.replace('ts_code=', '')
        for f in os.listdir(SIGNALS_DIR)
        if os.path.isdir(os.path.join(SIGNALS_DIR, f))
    ])
    sample_path = os.path.join(
        SIGNALS_DIR, f'ts_code={all_codes[0]}', 'part-0.parquet'
    )
    all_cols    = pq.read_schema(sample_path).names
    sync_fields = [c for c in all_cols if c not in EXCLUDE_COLS]

    logger.info(f"  同步字段数：{len(sync_fields) + 1}（含factor）")
    logger.info(f"  字段列表：{sync_fields + ['factor']}")

    cal_index = {date: idx for idx, date in enumerate(calendar)}
    total     = len(all_codes)
    logger.info(f"  待转换股票：{total} 只 | 并发线程：{max_workers}")
    os.makedirs(QLIB_FEAT, exist_ok=True)

    results   = []
    completed = 0
    t0        = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(_dump_single, code, cal_index, sync_fields): code
            for code in all_codes
        }
        for future in as_completed(future_map):
            completed += 1
            results.append(future.result())
            if completed % 500 == 0 or completed == total:
                elapsed = time.time() - t0
                logger.info(
                    f"  进度: {completed}/{total} "
                    f"({completed / total * 100:.1f}%) | "
                    f"速率: {completed / elapsed:.0f}只/秒 | "
                    f"耗时: {elapsed:.1f}s"
                )

    # ── 汇总 ──────────────────────────────────────────────────────
    report   = pd.DataFrame(results)
    pass_cnt = (report['status'] == 'PASS').sum()
    skip_cnt = (report['status'] == 'SKIP').sum()
    fail_df  = report[report['status'] == 'FAIL']

    elapsed = time.time() - t0
    logger.info(f"  ✅ 成功：{pass_cnt} 只")
    if skip_cnt:
        logger.info(f"  ⏭️  跳过：{skip_cnt} 只")
    if not fail_df.empty:
        logger.warning(f"  ❌ 失败：{len(fail_df)} 只")
        for _, row in fail_df.iterrows():
            logger.warning(f"    {row['ts_code']}  {row['note']}")
    logger.info(f"  总耗时：{elapsed:.1f}s")


# ------------------------------------------------------------------ #
#  主入口
# ------------------------------------------------------------------ #

def run_dump(max_workers: int = 20):
    t0 = time.time()
    logger.info("=" * 55)
    logger.info("Qlib 数据转换开始（从 step3_signals_table 读取）")
    logger.info(f"输入：{SIGNALS_DIR}")
    logger.info(f"输出：{os.path.abspath(QLIB_DIR)}")
    logger.info("=" * 55)

    calendar = dump_calendars()
    dump_instruments()
    dump_features(calendar, max_workers=max_workers)

    logger.info(f"\n{'=' * 55}")
    logger.info(f"✅ 全部完成 | 总耗时：{time.time() - t0:.1f}s")
    logger.info(f"{'=' * 55}")
    logger.info(f"\nQlib 初始化命令：")
    logger.info(f"  import qlib")
    logger.info(f"  qlib.init(provider_uri='{os.path.abspath(QLIB_DIR)}')")


# ------------------------------------------------------------------ #
#  调试入口
# ------------------------------------------------------------------ #

if __name__ == '__main__':
    logging.basicConfig(
        level   = logging.INFO,
        format  = '%(asctime)s [%(levelname)s] %(message)s',
        datefmt = '%H:%M:%S',
    )
    run_dump(max_workers=20)