"""
信号构建模块 signal_builder.py

基于因子层（step2_factors_table）构造策略形态信号（0/1）
结果存入 step3_signals_table

输入：datapipeline/factor/step2_factors_table/ts_code=xxx/
输出：datapipeline/factor/step3_signals_table/ts_code=xxx/

信号定义：

    kdj_cross     KDJ低位金叉
                  最近3天内任意一天满足：
                    今天 K > D（金叉发生）
                    昨天 K < D（刚刚发生，排除已金叉多日）
                    K <= 24 且 D <= 24（严格低位）

    rsi_new_low   RSI近期触底
                  近5日RSI最低点 <= 近10日RSI最低点
                  即：最低点出现在近期（最近5天）而不是更早之前

    macd_shrink   MACD负值缩量
                  连续3天递增 且 当天值 < 0.1（允许轻微转正）
                  还原原始逻辑：macd[-3] < macd[-2] < macd[-1] < 0.1

信号参数（来自原始策略网格搜索优选组）：
    KDJ_THRESH   = 24
    KDJ_WINDOW   = 3
    RSI_SHORT    = 5
    RSI_LOOKBACK = 5
    MACD_UPPER   = 0.1
    MACD_DAYS    = 3
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import time
import logging
import warnings
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from concurrent.futures import ProcessPoolExecutor, as_completed
from config import (
    DAILY_DIR, ADJ_DIR, BASIC_DIR,
    WIDE_DIR, FACTORS_DIR, SIGNALS_DIR, REPORT_PATH,
)

warnings.filterwarnings('ignore')
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------ #
#  信号参数
# ------------------------------------------------------------------ #

KDJ_THRESH   = 35     # K 和 D 都必须 <= 此值（严格低位）
KDJ_WINDOW   = 3      # 金叉窗口：最近N天内任意一天触发即标记为1

RSI_SHORT    = 5      # 近期窗口（天）
RSI_LOOKBACK = 5      # 额外回溯（总窗口 = SHORT + LOOKBACK = 10天）

MACD_UPPER   = 0.1    # MACD柱当天值必须 < 此值
MACD_DAYS    = 3      # 连续递增天数

SIGNAL_COLS  = ['kdj_cross', 'rsi_new_low', 'macd_shrink']
MIN_ROWS     = 80     # 最少行数（ma_60 需要60行，留一定余量）


# ------------------------------------------------------------------ #
#  信号计算
# ------------------------------------------------------------------ #

def calc_signals(df: pd.DataFrame) -> pd.DataFrame:
    """
    基于因子层数据构造 0/1 形态信号

    输入 df 必须包含：macd_bar / kdj_k / kdj_d / rsi_6
    df 已按 trade_date 升序排列
    """

    # ── KDJ 低位金叉 ─────────────────────────────────────────────
    # 今天 K > D（金叉）
    # 昨天 K < D（刚刚发生，排除已金叉多日）
    # K <= KDJ_THRESH 且 D <= KDJ_THRESH（严格低位）
    # 3天滚动窗口：最近KDJ_WINDOW天内任意一天触发即标记为1
    k = df['kdj_k']
    d = df['kdj_d']

    cross_today = (
        (k > d)                     &   # 今天金叉
        (k.shift(1) < d.shift(1))   &   # 昨天还是死叉
        (k <= KDJ_THRESH)           &   # K 在低位
        (d <= KDJ_THRESH)               # D 在低位
    )
    df['kdj_cross'] = (
        cross_today
        .rolling(window=KDJ_WINDOW, min_periods=1)
        .max()
        .fillna(0)
        .astype(np.int8)
    )

    # ── RSI 近期触底 ──────────────────────────────────────────────
    # 近5日RSI最低点 <= 近10日RSI最低点
    # 即：最低点出现在最近5天内，而不是更早之前
    # 参数来源：网格搜索主流区间 rsi_period=5~15，rsi_lookback=2~5
    rsi         = df['rsi_6']
    long_window = RSI_SHORT + RSI_LOOKBACK   # 10天

    min_rsi_short = rsi.rolling(window=RSI_SHORT,   min_periods=RSI_SHORT).min()
    min_rsi_long  = rsi.rolling(window=long_window, min_periods=long_window).min()

    df['rsi_new_low'] = (
        (min_rsi_short <= min_rsi_long)
        .fillna(False)
        .astype(np.int8)
    )

    # ── MACD 负值缩量 ─────────────────────────────────────────────
    # 严格还原原始逻辑：
    #   macd_bar[-3] < macd_bar[-2] < macd_bar[-1] < 0.1
    #   连续3天递增（负值区柱子在收缩）且当天值 < 0.1
    bar  = df['macd_bar']
    cond = bar < MACD_UPPER
    for i in range(1, MACD_DAYS):
        cond = cond & (bar > bar.shift(i))

    df['macd_shrink'] = cond.fillna(False).astype(np.int8)

    return df


# ------------------------------------------------------------------ #
#  单只股票处理
# ------------------------------------------------------------------ #

def _process_single(ts_code: str) -> dict:
    """
    处理单只股票：
        1. 读因子层数据（step2_factors_table）
        2. 计算三个形态信号
        3. 写出 signals parquet（保留全部列）
    """
    result = {'ts_code': ts_code, 'status': 'PASS', 'note': ''}

    try:
        # ── 读因子层全部列 ────────────────────────────────────────
        factor_path = os.path.join(FACTORS_DIR, f'ts_code={ts_code}')
        df = pq.read_table(factor_path).to_pandas()

        if df.empty:
            result['status'] = 'SKIP'
            result['note']   = '因子表为空'
            return result

        # ── 检查必要字段 ──────────────────────────────────────────
        required = {'trade_date', 'macd_bar', 'kdj_k', 'kdj_d', 'rsi_6'}
        missing  = required - set(df.columns)
        if missing:
            result['status'] = 'SKIP'
            result['note']   = f'缺少字段: {missing}'
            return result

        # ── 日期排序 ──────────────────────────────────────────────
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        df = df.sort_values('trade_date').reset_index(drop=True)

        # ── 数据量检查 ────────────────────────────────────────────
        if len(df) < MIN_ROWS:
            result['status'] = 'SKIP'
            result['note']   = f'数据量不足({len(df)}行)'
            return result

        # ── 计算信号 ──────────────────────────────────────────────
        df = calc_signals(df)

        # ── 写出 signals parquet ──────────────────────────────────
        out_dir = os.path.join(SIGNALS_DIR, f'ts_code={ts_code}')
        os.makedirs(out_dir, exist_ok=True)
        pq.write_table(
            pa.Table.from_pandas(df=df, preserve_index=False),
            os.path.join(out_dir, 'part-0.parquet'),
            compression='snappy',
        )

        # ── 统计信号触发次数 ──────────────────────────────────────
        n_kdj  = int(df['kdj_cross'].sum())
        n_rsi  = int(df['rsi_new_low'].sum())
        n_macd = int(df['macd_shrink'].sum())
        n_all  = int((
            (df['kdj_cross']   == 1) &
            (df['rsi_new_low'] == 1) &
            (df['macd_shrink'] == 1)
        ).sum())
        result['note'] = f'kdj={n_kdj} rsi={n_rsi} macd={n_macd} all3={n_all}'

    except Exception as e:
        result['status'] = 'FAIL'
        result['note']   = str(e)

    return result


# ------------------------------------------------------------------ #
#  主流程
# ------------------------------------------------------------------ #

def run(max_workers: int = 20):
    t0 = time.time()

    logger.info('=' * 65)
    logger.info('信号构建开始 signal_builder.py')
    logger.info(f'输入：{FACTORS_DIR}')
    logger.info(f'输出：{SIGNALS_DIR}')
    logger.info('信号参数：')
    logger.info(f'  kdj_cross   K上穿D 且 K<=D<={KDJ_THRESH}，{KDJ_WINDOW}天窗口')
    logger.info(f'  rsi_new_low 近{RSI_SHORT}日低点 <= 近{RSI_SHORT + RSI_LOOKBACK}日低点')
    logger.info(f'  macd_shrink 连续{MACD_DAYS}天递增 且 当天 < {MACD_UPPER}')
    logger.info('=' * 65)

    # 扫描因子目录
    all_codes = sorted([
        f.replace('ts_code=', '')
        for f in os.listdir(FACTORS_DIR)
        if os.path.isdir(os.path.join(FACTORS_DIR, f))
    ])
    total = len(all_codes)
    logger.info(f'待处理股票：{total} 只 | 并发进程：{max_workers}')
    os.makedirs(SIGNALS_DIR, exist_ok=True)

    # ── 并发处理 ──────────────────────────────────────────────────
    results   = []
    completed = 0

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(_process_single, code): code
            for code in all_codes
        }
        for future in as_completed(future_map):
            completed += 1
            results.append(future.result())
            if completed % 500 == 0 or completed == total:
                elapsed = time.time() - t0
                logger.info(
                    f'进度: {completed}/{total} '
                    f'({completed / total * 100:.1f}%) | '
                    f'速率: {completed / elapsed:.0f}只/秒 | '
                    f'耗时: {elapsed:.1f}s'
                )

    # ── 汇总结果 ──────────────────────────────────────────────────
    report   = pd.DataFrame(results)
    pass_cnt = (report['status'] == 'PASS').sum()
    skip_cnt = (report['status'] == 'SKIP').sum()
    fail_df  = report[report['status'] == 'FAIL']

    elapsed = time.time() - t0
    logger.info(f'\n{"=" * 65}')
    logger.info(f'信号构建完成 | 总耗时：{elapsed:.1f}s')
    logger.info(f'{"=" * 65}')
    logger.info(f'✅ 成功：{pass_cnt} 只')
    if skip_cnt:
        logger.info(f'⏭️  跳过：{skip_cnt} 只')
    if not fail_df.empty:
        logger.warning(f'❌ 失败：{len(fail_df)} 只')
        for _, row in fail_df.iterrows():
            logger.warning(f'   {row["ts_code"]}  {row["note"]}')
    logger.info(f'{"=" * 65}')

    # ── 验证：000001.SZ 信号统计 ──────────────────────────────────
    logger.info('\n验证输出（000001.SZ）：')
    try:
        sample = pq.read_table(
            os.path.join(SIGNALS_DIR, 'ts_code=000001.SZ')
        ).to_pandas()
        sample['trade_date'] = pd.to_datetime(sample['trade_date'])
        sample = sample.sort_values('trade_date')

        total_days = len(sample)
        n_kdj  = sample['kdj_cross'].sum()
        n_rsi  = sample['rsi_new_low'].sum()
        n_macd = sample['macd_shrink'].sum()
        n_all  = (
            (sample['kdj_cross']   == 1) &
            (sample['rsi_new_low'] == 1) &
            (sample['macd_shrink'] == 1)
        ).sum()

        logger.info(f'总交易日：{total_days}')
        logger.info(f'kdj_cross   触发：{n_kdj:4d} 次 ({n_kdj  / total_days * 100:.1f}%)')
        logger.info(f'rsi_new_low 触发：{n_rsi:4d} 次 ({n_rsi  / total_days * 100:.1f}%)')
        logger.info(f'macd_shrink 触发：{n_macd:4d} 次 ({n_macd / total_days * 100:.1f}%)')
        logger.info(f'三因子同时  触发：{n_all:4d} 次 ({n_all  / total_days * 100:.1f}%)')

        logger.info('\n最近10次三因子同时触发：')
        all3_rows = sample[
            (sample['kdj_cross']   == 1) &
            (sample['rsi_new_low'] == 1) &
            (sample['macd_shrink'] == 1)
        ][['trade_date', 'kdj_cross', 'rsi_new_low', 'macd_shrink',
           'kdj_k', 'kdj_d', 'rsi_6', 'macd_bar']]
        print(all3_rows.tail(10).to_string(index=False))

    except Exception as e:
        logger.warning(f'验证失败：{e}')


# ------------------------------------------------------------------ #
#  调试入口
# ------------------------------------------------------------------ #

if __name__ == '__main__':
    logging.basicConfig(
        level   = logging.INFO,
        format  = '%(asctime)s [%(levelname)s] %(message)s',
        datefmt = '%H:%M:%S',
    )
    run(max_workers=20)