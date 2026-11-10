import matplotlib.pyplot as plt
import numpy as np
import sys
import os
from matplotlib.ticker import PercentFormatter
import matplotlib.gridspec as gridspec

import re

# FicusDB, Geth, ChainKV, FicusDB-LRU, PrunusDB
color = ['#EA6B66', '#7EA6E0', '#97D077', '#FFB570', '#A680B8']
CACHE_SIZES = [1024, 2048, 4096, 8192, 16384]

def load_ficus_log(log_file):
    group_len = 12
    entry = []
    line_num = 0
    data = {"trpt": [], "ratio": []}
    for line in open(log_file):
        line_num += 1
        entry.append(line)
        if line_num % group_len == 0:
            trpt = float(entry[0].split()[3])
            ratio = float(entry[4].split()[2])
            data["trpt"].append(trpt)
            data["ratio"].append(ratio)
            entry = []
    return data

def load_geth_log(log_file):
    group_len = 4
    entry = []
    line_num = 0
    data = {"trpt": [], "ratio": []}
    for line in open(log_file):
        line_num += 1
        entry.append(line)
        if line_num % group_len == 0:
            trpt = float(entry[0].split()[3])
            ratio = float(entry[2].split()[4])
            data["trpt"].append(trpt)
            data["ratio"].append(ratio)
            entry = []
    return data

def load_chainkv_log(log_file):
    data = {"trpt": []}
    for line in open(log_file):
        trpt = float(line.split()[3])
        data["trpt"].append(trpt)
    return data

def load_prunus_log(log_file):
    # PrunusDB prints a variable number of stats lines per batch, so parse by
    # keyword: each "put:"/"get:" line starts a batch, and the object-store
    # "[store cache] ... hit_ratio: X" line that follows gives the hit ratio.
    data = {"trpt": [], "ratio": []}
    for line in open(log_file):
        if line.startswith(("put:", "get:", "init:")):
            data["trpt"].append(float(line.split()[3]))
        else:
            m = re.search(r"hit_ratio:\s*([0-9.]+|NaN|nan)", line)
            if m:
                data["ratio"].append(float(m.group(1)))
    return data

# (system, log loader, color, label for latest-version get, label for versioned get, label for put)
SYSTEMS = [
    ('ficus',   load_ficus_log,   color[0], 'ficus',       'ficus-ver',    'ficus'),
    ('geth',    load_geth_log,    color[1], 'geth-get',    'geth-vget',    'geth'),
    ('chainkv', load_chainkv_log, color[2], 'chainkv-get', 'chainkv-vget', 'chainkv'),
    ('prunus',  load_prunus_log,  color[4], 'prunus',      'prunus-ver',   'prunus'),
]

def log_path(system, key_size, ops, cache_size, val_size=200, batch_size=2000):
    if ops == 'put':
        return f"../logs/put/micro-put-{system}-{key_size}-50m-{cache_size}-{val_size}-{batch_size}.log"
    return f"../logs/{ops}/micro-{ops}-{system}-{key_size}-50m-{cache_size}.log"

def has_log(system, key_size, ops):
    return all(os.path.exists(log_path(system, key_size, ops, c)) for c in CACHE_SIZES)

def steady_trpt(system, loader, key_size, ops, warmup=0.8):
    # Mean throughput per cache size over the post-warmup tail of each log.
    out = []
    for c in CACHE_SIZES:
        t = np.array(loader(log_path(system, key_size, ops, c))['trpt'])
        out.append(np.mean(t[int(len(t)*warmup):]))
    return np.array(out)

def plot_bars(axs, series, ylim, legend_ncol):
    # series: list of (values, label, color, hatched); systems without logs are skipped.
    memsize = ['1G', '2G', '4G', '8G', '16G']
    index = np.arange(len(memsize))
    n_bars = len(series)
    bar_width = (0.78 if n_bars > 4 else 0.75) / max(n_bars, 1)
    offset = -bar_width*n_bars/2 + bar_width/2
    peak = 0
    for i, (vals, label, col, hatched) in enumerate(series):
        kw = dict(hatch='//', edgecolor='black') if hatched else {}
        axs.bar(index + bar_width*i + offset, vals/1000, bar_width, label=label, color=col, **kw)
        peak = max(peak, np.nanmax(vals)/1000)
    axs.set_ylim([0, max(ylim, 1.15*peak)])
    axs.set_xlabel('cache size')
    axs.set_ylabel('throughput (kops/s)')
    axs.set_xticks(list(index), memsize)
    if series:
        axs.legend(loc='upper left', ncol=legend_ncol)

def plot_get(axs, key_size, ylim, title):
    series = []
    for system, loader, col, get_label, vget_label, _ in SYSTEMS:
        if not (has_log(system, key_size, 'get') and has_log(system, key_size, 'vget')):
            print(f"plot_get {key_size}: skipping {system} (missing logs)", file=sys.stderr)
            continue
        series.append((steady_trpt(system, loader, key_size, 'vget'), vget_label, col, False))
        series.append((steady_trpt(system, loader, key_size, 'get'), get_label, col, True))
    plot_bars(axs, series, ylim, legend_ncol=2)
    axs.set_title(title)

def plot_put(axs, key_size, ylim, title):
    series = []
    for system, loader, col, _, _, put_label in SYSTEMS:
        if not has_log(system, key_size, 'put'):
            print(f"plot_put {key_size}: skipping {system} (missing logs)", file=sys.stderr)
            continue
        series.append((steady_trpt(system, loader, key_size, 'put'), put_label, col, False))
    plot_bars(axs, series, ylim, legend_ncol=1)
    axs.set_title(title)

def plot_batch(axs, title, color):
    warmup = 0.8
    val_size = [50, 200, 800, 3200]
    batch_size = [500, 2000, 8000, 32000]
    data = {}
    for vs in val_size:
        data[vs] = {}
        for bs in batch_size:
            log_file = f"../logs/put/micro-put-ficus-20m-50m-16384-{vs}-{bs}.log"
            data[vs][bs] = np.array(load_ficus_log(log_file)['trpt'])
    lines = []
    for vs in val_size:
        line = []
        for bs in batch_size:
            line.append(np.mean(data[vs][bs][int(len(data[vs][bs])*warmup):]))
        lines.append(np.array(line)/1000)

    axs.plot(lines[0], marker='v', label = 'val-50', color=color[0])
    axs.plot(lines[1], marker='o', label = 'val-200', color=color[1])
    axs.plot(lines[2], marker='d', label = 'val-800', color=color[2])
    axs.plot(lines[3], marker='s', label = 'val-3200', color=color[3])
    axs.set_xlabel('batch size (bytes)')
    axs.set_ylabel('throughput (kops/s)')
    axs.set_title(title)
    axs.legend(ncol=2)
    axs.set_xticks([0, 1, 2, 3], ['500', '2000', '8000', '32000'])

def plot_miss(axs, title):
    warmup = 0.8
    def miss_ratio(loader, files):
        out = []
        for f in files:
            r = np.array(loader(f)['ratio'])
            out.append(1 - np.nanmean(r[int(len(r)*warmup):]))
        return out
    lines = [  # (system, loader, key_size, log dir/op, label, color)
        ('ficus',  load_ficus_log,  '20m',  'get', 'ficus-20m',      color[0]),
        ('ficus',  load_ficus_log,  '100m', 'get', 'ficus-100m',     color[0]),
        ('geth',   load_geth_log,   '20m',  'get', 'geth-20m',       color[1]),
        ('geth',   load_geth_log,   '100m', 'get', 'geth-100m',      color[1]),
        ('ficus',  load_ficus_log,  '20m',  'lru', 'ficus-lru-20m',  color[3]),
        ('ficus',  load_ficus_log,  '100m', 'lru', 'ficus-lru-100m', color[3]),
        ('prunus', load_prunus_log, '20m',  'get', 'prunus-20m',     color[4]),
        ('prunus', load_prunus_log, '100m', 'get', 'prunus-100m',    color[4]),
    ]
    for system, loader, key_size, op, label, col in lines:
        if op == 'lru':
            files = [f"../logs/lru/micro-lru-{system}-{key_size}-50m-{c}.log" for c in CACHE_SIZES]
        else:
            files = [log_path(system, key_size, op, c) for c in CACHE_SIZES]
        if not all(os.path.exists(f) for f in files):
            print(f"plot_miss: skipping {label} (missing logs)", file=sys.stderr)
            continue
        marker, ls = ('v', '-') if key_size == '20m' else ('o', '--')
        axs.plot(miss_ratio(loader, files), marker=marker, label=label, color=col, linestyle=ls)
    axs.set_ylim([0, 0.33])
    axs.set_xlabel('cache size')
    axs.set_ylabel('miss ratio')
    axs.yaxis.set_major_formatter(PercentFormatter(1))
    axs.set_xticks([0, 1, 2, 3, 4], ['1G', '2G', '4G', '8G', '16G'])
    axs.legend(loc='upper right', ncol=2)
    axs.set_title(title)

def plot_micro(filename):
    fig = plt.figure(figsize=(10, 9))
    gx = gridspec.GridSpec(3, 6, figure=fig)
    getax1 = fig.add_subplot(gx[0, 0:3])
    getax2 = fig.add_subplot(gx[0, 3:6])
    plot_get(getax1, '20m', 350, '(a) Get Ops Throughput: 20M keys')
    plot_get(getax2, '100m', 250, '(b) Get Ops Throughput: 100M keys')
    putax1 = fig.add_subplot(gx[1, 0:2])
    putax2 = fig.add_subplot(gx[1, 2:4])
    plot_put(putax1, '20m', 42, '(c) Put Ops Throughput: 20M keys') 
    plot_put(putax2, '100m', 42, '(d) Put Ops Throughput: 100M keys')
    batchax = fig.add_subplot(gx[1, 4:6])
    plot_batch(batchax, '(e) Batch Write Throughput', color)
    missax = fig.add_subplot(gx[2, 0:3])
    plot_miss(missax, '(f) Cache Miss Ratio')

    fig.tight_layout()
    fig.savefig(filename, dpi=500)
    fig.clf()

plot_micro('micro-plot.png')