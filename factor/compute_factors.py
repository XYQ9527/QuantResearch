"""
因子计算模块 compute_factors.py

从宽表读取数据，用 TA-Lib 计算技术指标，结果存入 step2_factors

输入：data/factor/step1_wide/ts_code=xxx/
输出：data/factor/step2_factors/ts_code=xxx/

技术指标：
    MACD        macd_dif / macd_dea / macd_bar       (12, 26, 9)
    KDJ         kdj_k / kdj_d / kdj_j                (9, 3, 3)
    RSI         rsi_6 / rsi_12                       (6, 12)
    BOLL        boll_upper / boll_mid / boll_lower    (20, 2)
    动量         mom_5 / mom_10 / mom_20
    均线         ma_5 / ma_10 / ma_20 / ma_60
    波动率       atr_14
"""
import os
import sys
import time
import logging
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy          as np
import pandas         as pd
import pyarrow        as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import WIDE_DIR, FACTORS_DIR

warnings.filterwarnings('ignore')
logger = logging.getLogger(__name__)


# ================================================================== #
#  字段配置
# ================================================================== #

# 读宽表时排除原始未复权 OHLCV，其余全部保留
DROP_COLS = {'open', 'high', 'low', 'close', 'vol'}

# 新增技术指标字段（用于验证列数）
INDICATOR_COLS = [
    'macd_dif', 'macd_dea', 'macd_bar',
    'kdj_k',    'kdj_d',    'kdj_j',
    'rsi_6',    'rsi_12',
    'boll_upper', 'boll_mid', 'boll_lower',
    'mom_5',    'mom_10',   'mom_20',
    'ma_5',     'ma_10',    'ma_20',    'ma_60',
    'atr_14',
]


# ================================================================== #
#  技术指标计算
# ================================================================== #

def calc_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    用 TA-Lib 计算全部技术指标，追加到 df

    输入 df 必须包含：adj_close / adj_high / adj_low
    全部使用前复权价格，确保与交易软件对齐
    """
    import talib

    close = df['adj_close'].values.astype(np.float64)
    high  = df['adj_high'].values.astype(np.float64)
    low   = df['adj_low'].values.astype(np.float64)

    # ── MACD (12, 26, 9) ──────────────────────────────────────────
    macd_dif, macd_dea, macd_bar = talib.MACD(
        close, fastperiod=12, slowperiod=26, signalperiod=9
    )
    df['macd_dif'] = macd_dif
    df['macd_dea'] = macd_dea
    df['macd_bar'] = macd_bar * 2   # ×2 对齐通达信/同花顺显示标准

    # ── KDJ (9, 3, 3) ─────────────────────────────────────────────
    # TA-Lib STOCH：slowk=K, slowd=D，对应通达信/同花顺标准 KDJ
    kdj_k, kdj_d = talib.STOCH(
        high, low, close,
        fastk_period=9,
        slowk_matype=0,
        slowk_period=3,
        slowd_matype=0,
        slowd_period=3,
    )
    df['kdj_k'] = kdj_k
    df['kdj_d'] = kdj_d
    df['kdj_j'] = 3 * kdj_k - 2 * kdj_d

    # ── RSI (6, 12) ───────────────────────────────────────────────
    df['rsi_6']  = talib.RSI(close, timeperiod=6)
    df['rsi_12'] = talib.RSI(close, timeperiod=12)

    # ── BOLL (20, 2) ──────────────────────────────────────────────
    df['boll_upper'], df['boll_mid'], df['boll_lower'] = talib.BBANDS(
        close, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0
    )

    # ── 动量：N 日收益率 ──────────────────────────────────────────
    # pct_change(n) = (今日 - N日前) / N日前
    df['mom_5']  = df['adj_close'].pct_change(5).round(6)
    df['mom_10'] = df['adj_close'].pct_change(10).round(6)
    df['mom_20'] = df['adj_close'].pct_change(20).round(6)

    # ── 均线：收盘价简单移动平均 ──────────────────────────────────
    df['ma_5']  = talib.SMA(close, timeperiod=5)
    df['ma_10'] = talib.SMA(close, timeperiod=10)
    df['ma_20'] = talib.SMA(close, timeperiod=20)
    df['ma_60'] = talib.SMA(close, timeperiod=60)

    # ── ATR (14)：平均真实波幅，止损参考 ─────────────────────────
    df['atr_14'] = talib.ATR(high, low, close, timeperiod=14)

    return df


# ================================================================== #
#  单只股票处理
# ================================================================== #

def _process_single(ts_code: str) -> dict:
    """
    处理单只股票：
        1. 读宽表，排除原始未复权 OHLCV
        2. 计算技术指标
        3. 写出 factors parquet
    """
    result = {'ts_code': ts_code, 'status': 'PASS', 'note': ''}

    try:
        wide_path = os.path.join(WIDE_DIR, f'ts_code={ts_code}')

        # ── 读宽表，排除法过滤字段 ────────────────────────────────
        # 用 read_schema 只读元数据，避免两次完整 IO
        all_cols  = pq.read_schema(
            os.path.join(wide_path, 'part-0.parquet')
        ).names
        read_cols = [c for c in all_cols if c not in DROP_COLS]

        df = pq.read_table(wide_path, columns=read_cols).to_pandas()

        if df.empty:
            result['status'] = 'SKIP'
            result['note']   = '宽表为空'
            return result

        # ── 日期排序 ──────────────────────────────────────────────
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        df = df.sort_values('trade_date').reset_index(drop=True)

        # ── 数据量检查（ma_60 需要至少 60 行）───────────────────
        if len(df) < 60:
            result['status'] = 'SKIP'
            result['note']   = f'数据量不足({len(df)}行)'
            return result

        # ── 计算技术指标 ──────────────────────────────────────────
        df = calc_indicators(df)

        # ── 写出 factors parquet ──────────────────────────────────
        out_dir = os.path.join(FACTORS_DIR, f'ts_code={ts_code}')
        os.makedirs(out_dir, exist_ok=True)
        pq.write_table(
            pa.Table.from_pandas(df=df, preserve_index=False),
            os.path.join(out_dir, 'part-0.parquet'),
            compression='snappy',
        )

        result['note'] = f'{len(df)}行 | {len(INDICATOR_COLS)}个指标'

    except Exception as e:
        result['status'] = 'FAIL'
        result['note']   = str(e)

    return result


# ================================================================== #
#  主流程
# ================================================================== #

def run(max_workers: int = 20):
    t0 = time.time()

    logger.info('=' * 60)
    logger.info('因子计算开始 compute_factors.py')
    logger.info(f'输入：{WIDE_DIR}')
    logger.info(f'输出：{FACTORS_DIR}')
    logger.info('指标：MACD / KDJ / RSI / BOLL / MOM / MA / ATR')
    logger.info('=' * 60)

    all_codes = sorted([
        f.replace('ts_code=', '')
        for f in os.listdir(WIDE_DIR)
        if os.path.isdir(os.path.join(WIDE_DIR, f))
    ])
    total = len(all_codes)
    logger.info(f'待处理股票：{total} 只 | 并发进程：{max_workers}')
    os.makedirs(FACTORS_DIR, exist_ok=True)

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
    logger.info(f'\n{"=" * 60}')
    logger.info(f'因子计算完成 | 总耗时：{elapsed:.1f}s')
    logger.info(f'{"=" * 60}')
    logger.info(f'✅ 成功：{pass_cnt} 只')
    if skip_cnt:
        logger.info(f'⏭️  跳过：{skip_cnt} 只')
    if not fail_df.empty:
        logger.warning(f'❌ 失败：{len(fail_df)} 只')
        for _, row in fail_df.iterrows():
            logger.warning(f'   {row["ts_code"]}  {row["note"]}')
    logger.info(f'{"=" * 60}')

    # ── 验证输出（000001.SZ 最后5行）────────────────────────────
    logger.info('\n验证输出（000001.SZ 最后5行）：')
    try:
        sample = pq.read_table(
            os.path.join(FACTORS_DIR, 'ts_code=000001.SZ')
        ).to_pandas()
        sample['trade_date'] = pd.to_datetime(sample['trade_date'])
        sample = sample.sort_values('trade_date')

        verify_cols = [
            'trade_date', 'adj_close',
            'macd_bar', 'kdj_k', 'rsi_6',
            'mom_5', 'ma_20', 'atr_14',
        ]
        show_cols = [c for c in verify_cols if c in sample.columns]
        print(sample[show_cols].tail().to_string(index=False))
        logger.info(f'总列数：{len(sample.columns)} 列')
    except Exception as e:
        logger.warning(f'验证失败：{e}')


# ================================================================== #
#  调试入口
# ================================================================== #

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S',
    )
    run(max_workers=20)
