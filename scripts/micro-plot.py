import matplotlib.pyplot as plt
import numpy as np
import sys
import os
from matplotlib.ticker import PercentFormatter
import matplotlib.gridspec as gridspec

# FicusDB, Geth, ChainKV, FicusDB-LRU
color = ['#EA6B66', '#7EA6E0', '#97D077', '#FFB570']

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

def get_ficus_log(key_size, ops, item, val_size=200, batch_size=2000):
    data = []
    for cache_size in [1024, 2048, 4096, 8192, 16384]:
        if ops == 'put':
            log_file = f"../logs/put/micro-put-ficus-{key_size}-50m-{cache_size}-{val_size}-{batch_size}.log"
            data.append(np.array(load_ficus_log(log_file)[item]))
        else:
            log_file = f"../logs/{ops}/micro-{ops}-ficus-{key_size}-50m-{cache_size}.log"
            data.append(np.array(load_ficus_log(log_file)[item]))
    return data

def get_geth_log(key_size, ops, item, val_size=200, batch_size=2000):
    data = []
    for cache_size in [1024, 2048, 4096, 8192, 16384]:
        if ops == 'put':
            log_file = f"../logs/put/micro-put-geth-{key_size}-50m-{cache_size}-{val_size}-{batch_size}.log"
            data.append(np.array(load_geth_log(log_file)[item]))
        else:
            log_file = f"../logs/{ops}/micro-{ops}-geth-{key_size}-50m-{cache_size}.log"
            data.append(np.array(load_geth_log(log_file)[item]))
    return data

def get_chainkv_log(key_size, ops, item, val_size=200, batch_size=2000):
    data = []
    for cache_size in [1024, 2048, 4096, 8192, 16384]:
        if ops == 'put':
            log_file = f"../logs/put/micro-put-chainkv-{key_size}-50m-{cache_size}-{val_size}-{batch_size}.log"
            data.append(np.array(load_chainkv_log(log_file)[item]))
        else:
            log_file = f"../logs/{ops}/micro-{ops}-chainkv-{key_size}-50m-{cache_size}.log"
            data.append(np.array(load_chainkv_log(log_file)[item]))
    return data

def plot_get(axs, key_size, ylim, title):
    warmup = 0.8
    ficus_get = get_ficus_log(key_size, 'get', 'trpt')
    ficus_get = np.array([np.mean(t[int(len(t)*warmup):]) for t in ficus_get])
    ficus_vget = get_ficus_log(key_size, 'vget', 'trpt')
    ficus_vget = np.array([np.mean(t[int(len(t)*warmup):]) for t in ficus_vget])
    get_vget = get_geth_log(key_size, 'vget', 'trpt')
    get_vget = np.array([np.mean(t[int(len(t)*warmup):]) for t in get_vget])
    get_get = get_geth_log(key_size, 'get', 'trpt')
    get_get = np.array([np.mean(t[int(len(t)*warmup):]) for t in get_get])
    chainkv_vget = get_chainkv_log(key_size, 'vget', 'trpt')
    chainkv_vget = np.array([np.mean(t[int(len(t)*warmup):]) for t in chainkv_vget])
    chainkv_get = get_chainkv_log(key_size, 'get', 'trpt')
    chainkv_get = np.array([np.mean(t[int(len(t)*warmup):]) for t in chainkv_get])

    memsize = ['1G', '2G', '4G', '8G', '16G']
    n_groups = len(memsize)
    index = np.arange(n_groups)
    bar_width = 0.13
    offset = -bar_width*3+bar_width/2

    bar0 = axs.bar(index+bar_width*0+offset, ficus_vget/1000, bar_width, 
    label = 'ficus-ver', color=color[0])
    bar1 = axs.bar(index+bar_width*1+offset, ficus_get/1000, bar_width, 
    label = 'ficus', color=color[0], hatch='//', edgecolor='black')
    bar2 = axs.bar(index+bar_width*2+offset, get_vget/1000, bar_width, 
    label = 'geth-vget', color=color[1])
    bar3 = axs.bar(index+bar_width*3+offset, get_get/1000, bar_width, 
    label = 'geth-get', color=color[1], hatch='//', edgecolor='black')
    bar4 = axs.bar(index+bar_width*4+offset, chainkv_vget/1000, bar_width, 
    label = 'chainkv-vget', color=color[2])
    bar5 = axs.bar(index+bar_width*5+offset, chainkv_get/1000, bar_width, 
    label = 'chainkv-get', color=color[2], hatch='//', edgecolor='black')

    axs.set_ylim([0, ylim])
    axs.set_xlabel('cache size')
    axs.set_ylabel('throughput (kops/s)')
    axs.set_title(title)
    axs.set_xticks([0, 1, 2, 3, 4], memsize)
    axs.legend(loc='upper left', ncol=2)

def plot_put(axs, key_size, ylim, title):
    warmup = 0.8
    ficus_trpt = get_ficus_log(key_size, 'put', 'trpt')
    ficus_trpt = np.array([np.mean(t[int(len(t)*warmup):]) for t in ficus_trpt])
    
    geth_trpt = get_geth_log(key_size, 'put', 'trpt')
    geth_trpt = np.array([np.mean(t[int(len(t)*warmup):]) for t in geth_trpt])
    
    chainkv_trpt = get_chainkv_log(key_size, 'put', 'trpt')
    chainkv_trpt = np.array([np.mean(t[int(len(t)*warmup):]) for t in chainkv_trpt])
    
    memsize = ['1G', '2G', '4G', '8G', '16G']
    n_groups = len(memsize)
    index = np.arange(n_groups)
    bar_width = 0.25
    offset = -bar_width*3/2+bar_width/2

    
    bar0 = axs.bar(index+bar_width*0+offset, ficus_trpt/1000, bar_width, label = 'ficus', color=color[0])
    bar1 = axs.bar(index+bar_width*1+offset, geth_trpt/1000, bar_width, label = 'geth', color=color[1])
    bar2 = axs.bar(index+bar_width*2+offset, chainkv_trpt/1000, bar_width, label = 'chainkv', color=color[2])

    axs.set_ylim([0, ylim])
    axs.set_xlabel('cache size')
    axs.set_ylabel('throughput (kops/s)')
    axs.set_title(title)
    axs.set_xticks([0, 1, 2, 3, 4], memsize)
    axs.legend(loc='upper left')

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
    ficus_20m = get_ficus_log('20m', 'get', 'ratio')
    ficus_20m = np.array([1-np.mean(t[int(len(t)*warmup):]) for t in ficus_20m])
    ficus_100m = get_ficus_log('100m', 'get', 'ratio')
    ficus_100m = np.array([1-np.mean(t[int(len(t)*warmup):]) for t in ficus_100m])
    geth_20m = get_geth_log('20m', 'get', 'ratio')
    geth_20m = np.array([1-np.mean(t[int(len(t)*warmup):]) for t in geth_20m])
    geth_100m = get_geth_log('100m', 'get', 'ratio')
    geth_100m = np.array([1-np.mean(t[int(len(t)*warmup):]) for t in geth_100m])
    lru_20m = []
    for cache in [1024, 2048, 4096, 8192, 16384]:
        file_name = f"../logs/lru/micro-lru-ficus-20m-50m-{cache}.log"
        lru_20m.append(1-np.mean(np.array(load_ficus_log(file_name)['ratio'])[int(len(load_ficus_log(file_name)['ratio'])*warmup):]))
    lru_100m = []
    for cache in [1024, 2048, 4096, 8192, 16384]:
        file_name = f"../logs/lru/micro-lru-ficus-100m-50m-{cache}.log"
        lru_100m.append(1-np.mean(np.array(load_ficus_log(file_name)['ratio'])[int(len(load_ficus_log(file_name)['ratio'])*warmup):]))

    axs.plot(ficus_20m, marker='v', label = 'ficus-20m', color=color[0])
    axs.plot(ficus_100m, marker='o', label = 'ficus-100m', color=color[0], linestyle='--')
    axs.plot(geth_20m, marker='v', label = 'geth-20m', color=color[1])
    axs.plot(geth_100m, marker='o', label = 'geth-100m', color=color[1], linestyle='--')
    axs.plot(lru_20m, marker='v', label = 'ficus-lru-20m', color=color[3])
    axs.plot(lru_100m, marker='o', label = 'ficus-lru-100m', color=color[3], linestyle='--')
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