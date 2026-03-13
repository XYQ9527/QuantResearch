"""
因子评估模块 factor_eval.py

直接从 step3_signals_table 读取数据，不依赖 Qlib 表达式引擎
评估四个策略因子对未来5日和10日收益率的预测能力

评估因子：
    log_total_mv  小市值（负向）   → log(total_mv)
    rsi_new_low   RSI近期触底      → 0/1 信号
    kdj_cross     KDJ低位金叉      → 0/1 信号
    macd_shrink   MACD负值缩量     → 0/1 信号

输出：ic_dashboard.html（暗色交互式仪表板）
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import time
import logging
import warnings
import webbrowser
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from concurrent.futures import ProcessPoolExecutor, as_completed
from config import SIGNALS_DIR, FACTOR_ROOT

warnings.filterwarnings('ignore')
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------ #
#  参数配置
# ------------------------------------------------------------------ #

OUT_DIR     = FACTOR_ROOT
START_DATE  = '2020-01-01'
END_DATE    = '2026-01-01'
LABEL_DAYS  = [5, 10]
N_QUANTILES = 5

FACTOR_CONFIG = [
    ('log_total_mv', '小市值',      -1),
    ('rsi_new_low',  'RSI近期触底',  1),
    ('kdj_cross',    'KDJ低位金叉',  1),
    ('macd_shrink',  'MACD负值缩量', 1),
]
FACTOR_LIST  = [f for f, _, _ in FACTOR_CONFIG]
FACTOR_NAMES = {f: n for f, n, _ in FACTOR_CONFIG}
FACTOR_DIRS  = {f: d for f, _, d in FACTOR_CONFIG}

COLORS = {
    'log_total_mv': '#f97316',
    'rsi_new_low':  '#22c55e',
    'kdj_cross':    '#38bdf8',
    'macd_shrink':  '#a78bfa',
}


# ------------------------------------------------------------------ #
#  Step 1：读取数据
# ------------------------------------------------------------------ #

def _load_single(ts_code: str) -> pd.DataFrame:
    try:
        path = os.path.join(SIGNALS_DIR, f'ts_code={ts_code}')
        df   = pq.read_table(
            path,
            columns=[
                'trade_date', 'adj_close', 'total_mv',
                'rsi_new_low', 'kdj_cross', 'macd_shrink',
            ],
        ).to_pandas()
        df['ts_code'] = ts_code
        return df
    except Exception:
        return pd.DataFrame()


def load_data(max_workers: int = 20) -> pd.DataFrame:
    logger.info(f'读取数据：{SIGNALS_DIR}')
    all_codes = sorted([
        f.replace('ts_code=', '')
        for f in os.listdir(SIGNALS_DIR)
        if os.path.isdir(os.path.join(SIGNALS_DIR, f))
    ])
    logger.info(f'股票数：{len(all_codes)} 只')

    dfs = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_load_single, code): code
            for code in all_codes
        }
        for future in as_completed(futures):
            df = future.result()
            if not df.empty:
                dfs.append(df)

    logger.info('合并数据...')
    df = pd.concat(dfs, ignore_index=True)
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df = df[
        (df['trade_date'] >= START_DATE) &
        (df['trade_date'] <  END_DATE)
    ].copy()
    df = df.sort_values(['ts_code', 'trade_date']).reset_index(drop=True)

    # ── 构造衍生因子 ──────────────────────────────────────────────
    logger.info('构造衍生因子...')
    df['log_total_mv'] = np.log(df['total_mv'].replace(0, np.nan))

    # ── 构造未来收益率标签 ────────────────────────────────────────
    for n in LABEL_DAYS:
        label = f'label_{n}d'
        logger.info(f'构造标签：未来{n}日收益...')
        df[label] = (
            df.groupby('ts_code')['adj_close']
            .transform(lambda x: x.shift(-n) / x - 1)
        )

    df = df.set_index(['trade_date', 'ts_code'])
    logger.info(f'数据形状：{df.shape}')
    return df


# ------------------------------------------------------------------ #
#  Step 2：IC 计算
# ------------------------------------------------------------------ #

def calc_ic(df: pd.DataFrame, factor: str, label: str) -> pd.Series:
    """按交易日截面计算 Spearman IC"""
    direction = FACTOR_DIRS.get(factor, 1)

    def _ic_one_day(group):
        f_vals = group[factor] * direction
        l_vals = group[label]
        mask   = f_vals.notna() & l_vals.notna()
        if mask.sum() < 30:
            return np.nan
        return f_vals[mask].corr(l_vals[mask], method='spearman')

    ic = df.groupby(level='trade_date').apply(_ic_one_day)
    ic.name = factor
    return ic.dropna()


def ic_stats(ic: pd.Series) -> dict:
    if len(ic) == 0:
        return {}
    icir = ic.mean() / ic.std() if ic.std() > 0 else 0
    return {
        'IC均值':    round(ic.mean(), 4),
        'IC标准差':  round(ic.std(),  4),
        'ICIR':      round(icir,      4),
        'IC>0胜率':  round((ic > 0).mean(), 4),
        '|IC|>0.02': round((ic.abs() > 0.02).mean(), 4),
        '样本天数':  len(ic),
    }


# ------------------------------------------------------------------ #
#  Step 3：分组收益
# ------------------------------------------------------------------ #

def calc_quantile_return(
    df: pd.DataFrame, factor: str, label: str
) -> pd.DataFrame:
    """按因子值分5组，计算各组平均未来收益率"""
    direction = FACTOR_DIRS.get(factor, 1)
    results   = []

    for date, group in df.groupby(level='trade_date'):
        f_vals = group[factor] * direction
        l_vals = group[label]
        mask   = f_vals.notna() & l_vals.notna()
        g      = pd.DataFrame({
            'factor': f_vals[mask],
            'label':  l_vals[mask],
        })
        if len(g) < N_QUANTILES * 2:
            continue
        # 0/1信号唯一值不足5个，跳过分组
        if g['factor'].nunique() < N_QUANTILES:
            continue
        try:
            g['q'] = pd.qcut(
                g['factor'], N_QUANTILES,
                labels=range(1, N_QUANTILES + 1),
                duplicates='drop',
            )
            q_ret      = g.groupby('q', observed=True)['label'].mean()
            q_ret.name = date
            results.append(q_ret)
        except Exception:
            continue

    return pd.DataFrame(results) if results else pd.DataFrame()


# ------------------------------------------------------------------ #
#  Step 4：生成 HTML 仪表板
# ------------------------------------------------------------------ #

def export_html(
    all_stats:  dict,
    all_ic:     dict,
    all_q_ret:  dict,
    out_dir:    str,
) -> str:
    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
    except ImportError:
        logger.error('请先安装 plotly：pip install plotly')
        return ''

    fill_color_map = {
        'log_total_mv': 'rgba(249,115,22,0.15)',
        'rsi_new_low':  'rgba(34,197,94,0.15)',
        'kdj_cross':    'rgba(56,189,248,0.15)',
        'macd_shrink':  'rgba(167,139,250,0.15)',
    }

    # ── Tab1：单因子深度分析（5日 + 10日双视图）──────────────────
    fig1 = make_subplots(
        rows=4, cols=2,
        subplot_titles=(
            'IC 时序（5日）',    'IC 时序（10日）',
            '累计 IC',           '60日滚动 IC 均值',
            '分组收益（5日）',   '分组收益（10日）',
            'IC 分布直方图',     '统计指标',
        ),
        vertical_spacing=0.08,
        horizontal_spacing=0.10,
    )

    all_trace_groups = {}
    trace_idx        = 0
    factors          = list(all_stats.keys())
    best = max(
        factors,
        key=lambda f: abs(all_stats[f].get('5d', {}).get('ICIR', 0)),
    )

    for factor in factors:
        color     = COLORS.get(factor, '#888')
        visible   = (factor == best)
        group_traces = []

        for period_key, col in [('5d', 1), ('10d', 2)]:
            label  = f'label_{period_key}'
            ic     = all_ic[factor][period_key]
            dates  = [str(x)[:10] for x in ic.index]
            ic_mean = ic.mean()

            # IC 柱状图
            fig1.add_trace(go.Bar(
                x=dates, y=ic.values,
                marker_color=[
                    color if v > 0 else '#ef4444' for v in ic.values
                ],
                marker_opacity=0.6,
                visible=visible, showlegend=False,
                hovertemplate='%{x}<br>IC: %{y:.4f}<extra></extra>',
            ), row=1, col=col)
            group_traces.append(trace_idx); trace_idx += 1

            # IC 均值参考线
            fig1.add_trace(go.Scatter(
                x=[dates[0], dates[-1]], y=[ic_mean, ic_mean],
                mode='lines',
                line=dict(color=color, dash='dash', width=1.5),
                name=f'均值={ic_mean:.4f}',
                visible=visible,
            ), row=1, col=col)
            group_traces.append(trace_idx); trace_idx += 1

        # 累计 IC（5日和10日叠加）
        for period_key, dash in [('5d', 'solid'), ('10d', 'dot')]:
            ic    = all_ic[factor][period_key]
            dates = [str(x)[:10] for x in ic.index]
            fig1.add_trace(go.Scatter(
                x=dates, y=ic.cumsum().values,
                mode='lines', name=f'累计IC {period_key}',
                line=dict(color=color, width=2, dash=dash),
                fill='tozeroy' if dash == 'solid' else None,
                fillcolor=fill_color_map.get(factor, ''),
                visible=visible, showlegend=False,
            ), row=2, col=1)
            group_traces.append(trace_idx); trace_idx += 1

        # 60日滚动 IC
        for period_key, dash in [('5d', 'solid'), ('10d', 'dot')]:
            ic    = all_ic[factor][period_key]
            dates = [str(x)[:10] for x in ic.index]
            fig1.add_trace(go.Scatter(
                x=dates, y=ic.rolling(60, min_periods=10).mean().values,
                mode='lines', name=f'滚动IC {period_key}',
                line=dict(color=color, width=2, dash=dash),
                visible=visible, showlegend=False,
            ), row=2, col=2)
            group_traces.append(trace_idx); trace_idx += 1

        # 分组收益（5日 + 10日）
        for period_key, col in [('5d', 1), ('10d', 2)]:
            q_ret = all_q_ret[factor][period_key]
            if not q_ret.empty:
                q_mean   = q_ret.mean()
                q_labels = [f'Q{i}' for i in q_mean.index]
                q_vals   = (q_mean.values * 100).tolist()
                q_clrs   = [
                    '#15803d', '#86efac', '#fef08a', '#fca5a5', '#dc2626'
                ]
                fig1.add_trace(go.Bar(
                    x=q_labels, y=q_vals,
                    marker_color=q_clrs[:len(q_labels)],
                    text=[f'{v:.3f}%' for v in q_vals],
                    textposition='outside',
                    visible=visible, showlegend=False,
                ), row=3, col=col)
                group_traces.append(trace_idx); trace_idx += 1

        # IC 分布直方图（5日）
        ic5 = all_ic[factor]['5d']
        fig1.add_trace(go.Histogram(
            x=ic5.values, nbinsx=60,
            marker_color=color, opacity=0.7,
            visible=visible, showlegend=False,
        ), row=4, col=1)
        group_traces.append(trace_idx); trace_idx += 1

        # 统计指标文字
        s5   = all_stats[factor]['5d']
        s10  = all_stats[factor]['10d']
        icir = s5['ICIR']
        validity = (
            '🟢 强有效' if abs(icir) >= 1.0 else
            '🟢 有效'   if abs(icir) >= 0.5 else
            '🟡 弱有效' if abs(icir) >= 0.3 else
            '🔴 无效'
        )
        dir_label = '反向因子' if FACTOR_DIRS.get(factor, 1) == -1 else '正向因子'
        table_text = (
            f"<b>{FACTOR_NAMES.get(factor, factor)}</b><br><br>"
            f"<b>── 5日 ──</b><br>"
            f"IC均值：{s5['IC均值']:.4f}<br>"
            f"ICIR：{s5['ICIR']:.4f}<br>"
            f"IC>0胜率：{s5['IC>0胜率']:.1%}<br><br>"
            f"<b>── 10日 ──</b><br>"
            f"IC均值：{s10['IC均值']:.4f}<br>"
            f"ICIR：{s10['ICIR']:.4f}<br>"
            f"IC>0胜率：{s10['IC>0胜率']:.1%}<br><br>"
            f"有效性：{validity}<br>"
            f"方向：{dir_label}"
        )
        fig1.add_trace(go.Scatter(
            x=[0.5], y=[0.5], mode='text', text=[table_text],
            textfont=dict(size=12, color='#e2e8f0'),
            visible=visible, showlegend=False, hoverinfo='skip',
        ), row=4, col=2)
        group_traces.append(trace_idx); trace_idx += 1

        all_trace_groups[factor] = group_traces

    # 切换按钮
    total_traces = trace_idx
    buttons = []
    for factor in factors:
        vis = [False] * total_traces
        for idx in all_trace_groups[factor]:
            vis[idx] = True
        buttons.append(dict(
            label=FACTOR_NAMES.get(factor, factor),
            method='update',
            args=[
                {'visible': vis},
                {'title': f'单因子深度分析：{FACTOR_NAMES.get(factor, factor)}'},
            ],
        ))

    fig1.update_layout(
        title=f'单因子深度分析：{FACTOR_NAMES.get(best, best)}',
        updatemenus=[dict(
            buttons=buttons, direction='down', showactive=True,
            x=0.0, y=1.08, xanchor='left',
            bgcolor='#1e293b', bordercolor='#334155',
            font=dict(color='#e2e8f0'),
        )],
        template='plotly_dark', height=1100,
        paper_bgcolor='#0d1117', plot_bgcolor='#0d1117',
        font=dict(family='JetBrains Mono, monospace', color='#e2e8f0'),
        margin=dict(t=120, b=40, l=60, r=40),
    )
    fig1.update_xaxes(showticklabels=False, showgrid=False, row=4, col=2)
    fig1.update_yaxes(showticklabels=False, showgrid=False, row=4, col=2)

    # ── Tab2：多因子对比 ──────────────────────────────────────────
    fig2 = make_subplots(
        rows=2, cols=2,
        subplot_titles=(
            '各因子累计 IC（5日）', '各因子累计 IC（10日）',
            '|ICIR| 对比（5日 vs 10日）', 'IC>0 胜率对比',
        ),
        vertical_spacing=0.12,
        horizontal_spacing=0.10,
    )

    for factor in factors:
        color     = COLORS.get(factor, '#888')
        disp_name = FACTOR_NAMES.get(factor, factor)
        ic5  = all_ic[factor]['5d']
        ic10 = all_ic[factor]['10d']
        d5   = [str(x)[:10] for x in ic5.index]
        d10  = [str(x)[:10] for x in ic10.index]

        fig2.add_trace(go.Scatter(
            x=d5, y=ic5.cumsum().values,
            mode='lines', name=disp_name,
            line=dict(color=color, width=2),
        ), row=1, col=1)

        fig2.add_trace(go.Scatter(
            x=d10, y=ic10.cumsum().values,
            mode='lines', name=disp_name,
            line=dict(color=color, width=2),
            showlegend=False,
        ), row=1, col=2)

    disp_names = [FACTOR_NAMES.get(f, f) for f in factors]
    icir5  = [abs(all_stats[f]['5d']['ICIR'])  for f in factors]
    icir10 = [abs(all_stats[f]['10d']['ICIR']) for f in factors]

    fig2.add_trace(go.Bar(
        x=disp_names, y=icir5,
        name='5日 ICIR',
        marker_color=[COLORS.get(f, '#888') for f in factors],
        opacity=0.9,
        text=[f'{v:.3f}' for v in icir5],
        textposition='outside',
    ), row=2, col=1)
    fig2.add_trace(go.Bar(
        x=disp_names, y=icir10,
        name='10日 ICIR',
        marker_color=[COLORS.get(f, '#888') for f in factors],
        opacity=0.5,
        text=[f'{v:.3f}' for v in icir10],
        textposition='outside',
    ), row=2, col=1)
    fig2.add_hline(
        y=1.0, line_dash='dash', line_color='#22c55e',
        annotation_text='强有效 ≥1.0', row=2, col=1,
    )
    fig2.add_hline(
        y=0.5, line_dash='dash', line_color='#eab308',
        annotation_text='有效 ≥0.5', row=2, col=1,
    )

    win5  = [all_stats[f]['5d']['IC>0胜率']  for f in factors]
    win10 = [all_stats[f]['10d']['IC>0胜率'] for f in factors]
    fig2.add_trace(go.Bar(
        x=disp_names, y=win5,
        name='5日胜率',
        marker_color=[COLORS.get(f, '#888') for f in factors],
        opacity=0.9,
        text=[f'{v:.1%}' for v in win5],
        textposition='outside',
    ), row=2, col=2)
    fig2.add_trace(go.Bar(
        x=disp_names, y=win10,
        name='10日胜率',
        marker_color=[COLORS.get(f, '#888') for f in factors],
        opacity=0.5,
        text=[f'{v:.1%}' for v in win10],
        textposition='outside',
    ), row=2, col=2)
    fig2.add_hline(
        y=0.6, line_dash='dash', line_color='#22c55e',
        annotation_text='胜率 60%', row=2, col=2,
    )

    fig2.update_layout(
        title='多因子 IC 对比分析',
        template='plotly_dark', height=700,
        paper_bgcolor='#0d1117', plot_bgcolor='#0d1117',
        font=dict(family='JetBrains Mono, monospace', color='#e2e8f0'),
        legend=dict(bgcolor='#1e293b', bordercolor='#334155'),
        margin=dict(t=80, b=40, l=60, r=40),
        barmode='group',
    )

    # ── Tab3：汇总表格 ────────────────────────────────────────────
    headers = [
        '因子', '方向',
        'IC均值(5d)', 'ICIR(5d)', 'IC>0胜率(5d)',
        'IC均值(10d)', 'ICIR(10d)', 'IC>0胜率(10d)',
        '有效性',
    ]
    rows_data  = {h: [] for h in headers}
    row_colors = []

    for f in factors:
        s5   = all_stats[f]['5d']
        s10  = all_stats[f]['10d']
        icir = s5['ICIR']
        rows_data['因子'].append(FACTOR_NAMES.get(f, f))
        rows_data['方向'].append(
            '反向' if FACTOR_DIRS.get(f, 1) == -1 else '正向'
        )
        rows_data['IC均值(5d)'].append(f"{s5['IC均值']:.4f}")
        rows_data['ICIR(5d)'].append(f"{s5['ICIR']:.4f}")
        rows_data['IC>0胜率(5d)'].append(f"{s5['IC>0胜率']:.1%}")
        rows_data['IC均值(10d)'].append(f"{s10['IC均值']:.4f}")
        rows_data['ICIR(10d)'].append(f"{s10['ICIR']:.4f}")
        rows_data['IC>0胜率(10d)'].append(f"{s10['IC>0胜率']:.1%}")
        rows_data['有效性'].append(
            '强有效 ✅' if abs(icir) >= 1.0 else
            '有效 ✅'   if abs(icir) >= 0.5 else
            '弱有效 ⚠️' if abs(icir) >= 0.3 else
            '无效 ❌'
        )
        row_colors.append(
            '#1a3a2a' if f == best else '#0f172a'
        )

    fig3 = go.Figure(data=[go.Table(
        header=dict(
            values=[f'<b>{h}</b>' for h in headers],
            fill_color='#1e293b',
            font=dict(color='#94a3b8', size=12),
            align='center', height=36,
            line_color='#334155',
        ),
        cells=dict(
            values=[rows_data[h] for h in headers],
            fill_color=[row_colors] * len(headers),
            font=dict(color='#e2e8f0', size=12),
            align='center', height=32,
            line_color='#1e293b',
        ),
    )])
    best_s5 = all_stats[best]['5d']
    fig3.update_layout(
        title=(
            f'因子汇总 | 最优：{FACTOR_NAMES.get(best, best)}  '
            f'IC(5d)={best_s5["IC均值"]:.4f}  '
            f'ICIR(5d)={best_s5["ICIR"]:.4f}'
        ),
        template='plotly_dark', height=280,
        paper_bgcolor='#0d1117',
        font=dict(family='JetBrains Mono, monospace', color='#e2e8f0'),
        margin=dict(t=60, b=20, l=20, r=20),
    )

    # ── 拼接 HTML ─────────────────────────────────────────────────
    html1 = fig1.to_html(full_html=False, include_plotlyjs='cdn',  div_id='fig1')
    html2 = fig2.to_html(full_html=False, include_plotlyjs=False,  div_id='fig2')
    html3 = fig3.to_html(full_html=False, include_plotlyjs=False,  div_id='fig3')

    factor_str = ' · '.join(FACTOR_NAMES.get(f, f) for f in factors)
    style = """
    <style>
      * { box-sizing:border-box; margin:0; padding:0; }
      body { background:#090e1a; font-family:'JetBrains Mono',monospace; color:#e2e8f0; }
      .header { padding:20px 28px 14px; border-bottom:1px solid #1e293b; }
      .header h1 { font-size:20px; font-weight:800; }
      .header p  { font-size:12px; color:#475569; margin-top:4px; }
      .badge { display:inline-block; font-size:11px; padding:2px 10px;
               border-radius:4px; margin-left:10px; vertical-align:middle; }
      .badge-green { background:rgba(34,197,94,.15); color:#22c55e;
                     border:1px solid rgba(34,197,94,.3); }
      .badge-blue  { background:rgba(56,189,248,.1);  color:#38bdf8;
                     border:1px solid rgba(56,189,248,.2); }
      .tabs { display:flex; gap:6px; padding:14px 28px; background:#090e1a;
              border-bottom:1px solid #1e293b; position:sticky; top:0; z-index:99; }
      .tab  { padding:7px 20px; border-radius:6px; font-size:12px; cursor:pointer;
              border:1px solid #1e293b; background:#0f172a; color:#475569;
              font-family:inherit; }
      .tab.active { border-color:#22c55e; background:rgba(34,197,94,.1);
                    color:#22c55e; }
      .section { display:none; padding:16px 20px; }
      .section.active { display:block; }
      .hint { font-size:11px; color:#334155; padding:6px 28px 0; }
    </style>
    <script>
      function switchTab(id) {
        document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
        document.querySelectorAll('.section').forEach(s => s.classList.remove('active'));
        document.getElementById('tab-' + id).classList.add('active');
        document.getElementById('sec-' + id).classList.add('active');
        window.dispatchEvent(new Event('resize'));
      }
    </script>"""

    html_full = f"""<!DOCTYPE html>
<html lang="zh">
<head>
  <meta charset="utf-8">
  <title>IC Factor Lab</title>
  {style}
</head>
<body>
  <div class="header">
    <h1>IC Factor Lab
      <span class="badge badge-green">
        A股 {START_DATE[:4]}-{END_DATE[:4]}
      </span>
      <span class="badge badge-blue">超卖反弹策略</span>
    </h1>
    <p>
      因子：{factor_str} ·
      标签：未来5日 / 10日收益 ·
      Spearman IC
    </p>
  </div>
  <div class="tabs">
    <button class="tab active" id="tab-1" onclick="switchTab(1)">
      单因子深度分析
    </button>
    <button class="tab" id="tab-2" onclick="switchTab(2)">
      多因子对比
    </button>
    <button class="tab" id="tab-3" onclick="switchTab(3)">
      因子汇总表
    </button>
  </div>
  <p class="hint">
    鼠标悬停查看数值 · 框选放大 · 双击还原 · 左上角下拉切换因子
  </p>
  <div class="section active" id="sec-1">{html1}</div>
  <div class="section"        id="sec-2">{html2}</div>
  <div class="section"        id="sec-3">{html3}</div>
</body>
</html>"""

    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, 'ic_dashboard.html')
    with open(out_path, 'w', encoding='utf-8') as fh:
        fh.write(html_full)
    logger.info(f'仪表板已生成：{out_path}')
    return out_path


# ------------------------------------------------------------------ #
#  主流程
# ------------------------------------------------------------------ #

def run(open_browser: bool = True):
    t0 = time.time()
    logger.info('=' * 60)
    logger.info('因子评估开始 factor_eval.py')
    logger.info(f'因子：{[FACTOR_NAMES[f] for f in FACTOR_LIST]}')
    logger.info(f'时间：{START_DATE} ~ {END_DATE}')
    logger.info(f'收益期：{LABEL_DAYS}')
    logger.info('=' * 60)

    logger.info('\n[1/4] 读取数据...')
    df = load_data()

    all_ic, all_stats, all_q_ret = {}, {}, {}

    logger.info('\n[2/4] 计算 IC...')
    for factor in FACTOR_LIST:
        if factor not in df.columns:
            logger.warning(f'  {factor} 不在数据中，跳过')
            continue

        logger.info(f'\n  {FACTOR_NAMES[factor]}')
        all_ic[factor]    = {}
        all_stats[factor] = {}
        all_q_ret[factor] = {}

        for n in LABEL_DAYS:
            label = f'label_{n}d'
            key   = f'{n}d'
            ic    = calc_ic(df, factor, label)
            stats = ic_stats(ic)
            q_ret = calc_quantile_return(df, factor, label)

            all_ic[factor][key]    = ic
            all_stats[factor][key] = stats
            all_q_ret[factor][key] = q_ret

            icir     = stats.get('ICIR', 0)
            validity = (
                '强有效' if abs(icir) >= 1.0 else
                '有效'   if abs(icir) >= 0.5 else
                '弱有效' if abs(icir) >= 0.3 else
                '无效'
            )
            logger.info(
                f'    ret_{n}d  '
                f'IC={stats["IC均值"]:+.4f}  '
                f'ICIR={icir:+.4f}  '
                f'胜率={stats["IC>0胜率"]:.1%}  '
                f'{validity}'
            )

    logger.info('\n[3/4] 打印汇总...')
    rows = []
    for f in all_stats:
        s5  = all_stats[f]['5d']
        s10 = all_stats[f]['10d']
        rows.append({
            '因子':        FACTOR_NAMES.get(f, f),
            'IC(5d)':      f"{s5['IC均值']:+.4f}",
            'ICIR(5d)':    f"{s5['ICIR']:+.4f}",
            '胜率(5d)':    f"{s5['IC>0胜率']:.1%}",
            'IC(10d)':     f"{s10['IC均值']:+.4f}",
            'ICIR(10d)':   f"{s10['ICIR']:+.4f}",
            '胜率(10d)':   f"{s10['IC>0胜率']:.1%}",
        })
    for line in pd.DataFrame(rows).to_string(index=False).split('\n'):
        logger.info(line)

    logger.info('\n[4/4] 生成仪表板...')
    html_path = export_html(all_stats, all_ic, all_q_ret, OUT_DIR)
    if open_browser and html_path:
        webbrowser.open(f'file:///{html_path.replace(os.sep, "/")}')

    logger.info(f'\n✅ 完成 → {html_path}')
    logger.info(f'总耗时：{time.time() - t0:.1f}s')
    logger.info('=' * 60)


# ------------------------------------------------------------------ #
#  调试入口
# ------------------------------------------------------------------ #

if __name__ == '__main__':
    logging.basicConfig(
        level   = logging.INFO,
        format  = '%(asctime)s [%(levelname)s] %(message)s',
        datefmt = '%H:%M:%S',
    )
    run()