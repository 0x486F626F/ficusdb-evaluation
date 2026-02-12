import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import PercentFormatter, FuncFormatter, LogLocator

color = ['#EA6B66', '#7EA6E0', '#97D077', '#FFB570']

def load_ficus_log(log_file):
    group_len = 19
    entry = []
    line_num = 0
    data = {"trpt": [], "ratio": [], "time": [], "opget": [], "opput": []}
    for line in open(log_file):
        line_num += 1
        entry.append(line)
        if line_num % group_len == 0:
            trpt = float(entry[3].split()[3])
            ratio = float(entry[11].split()[2])
            time = float(entry[1].split()[1])
            opget = int(entry[3].split()[0])
            opput = int(entry[3].split()[1])
            data["trpt"].append(trpt)
            data["ratio"].append(ratio)
            data["time"].append(time)
            data["opget"].append(opget)
            data["opput"].append(opput)
            entry = []
    return data

def load_geth_log(log_file):
    group_len = 5
    entry = []
    line_num = 0
    data = {"trpt": [], "ratio": [], "time": []}
    for line in open(log_file):
        line_num += 1
        entry.append(line)
        if line_num % group_len == 0:
            trpt = float(entry[0].split()[4])
            ratio = float(entry[3].split()[4])
            time = float(entry[0].split()[1])
            data["trpt"].append(trpt)
            data["ratio"].append(ratio)
            data["time"].append(time)
            entry = []
    return data

ficus_4 = load_ficus_log('../logs/statedb/ficus-statedb-4096.log')
# ficus_32 = load_ficus_log('../logs/statedb/ficus-statedb-32768.log')
geth_4 = load_geth_log('../logs/statedb/geth-statedb-4096.log')
# geth_32 = load_ficus_log('../logs/statedb/geth-statedb-32768.log')

def window_ave(a, w=10):
    r = []
    for i in range(0, len(a)+w, w):
        r.append(sum(a[i:i+w])/w)
    return r[:-1]


def plot_block_time(axs, title):
    ficus_4_time = np.array(window_ave(ficus_4["time"], w=100))
    # ficus_32_time = np.array(load_ficus_log('../logs/statedb/ficus-statedb-32768.log')["time"])
    geth_4_time = np.array(window_ave(geth_4["time"], w=10))/10
    # geth_32_time = np.array(load_ficus_log('../logs/statedb/geth-statedb-32768.log')["time"])
    n = len(ficus_4_time)
    x = [10000000+i*(2000000/n) for i in range(n)]

    start = int(n*0.25)
    p0,=axs.plot(x[start:], ficus_4_time[start:], color=color[0], label='ficus-4g', linestyle='solid')
    p2,=axs.plot(x[start:], geth_4_time[start:], color=color[1], label='geth-4g', linestyle='solid')

    axs.set_title(title)
    axs.set_ylabel('time (ms)')
    axs.set_xlabel('block number')
    axs.legend(ncol=2)

def plot_trpt(axs, title):
    ficus_4_trpt = np.array(window_ave(ficus_4["trpt"], w=100))/1000
    geth_4_trpt = np.array(window_ave(geth_4["trpt"], w=10))/1000
    n = len(ficus_4_trpt)
    x = [10000000+i*(2000000/n) for i in range(n)]
    start = int(n*0.25)
    p0,=axs.plot(x[start:], ficus_4_trpt[start:], color=color[0], label='ficus-4g', linestyle='solid')
    p2,=axs.plot(x[start:], geth_4_trpt[start:], color=color[1], label='geth-4g', linestyle='solid')
    axs.set_title(title)
    axs.set_ylabel('throughput (kops/s)')
    axs.set_xlabel('block number')
    axs.legend(ncol=2)

def plot_ops(axs, title):
    get_ops = np.array(window_ave(ficus_4["opget"], w=10))/1000
    put_ops = np.array(window_ave(ficus_4["opput"], w=10))/1000
    n = len(get_ops)
    x = [10000000+i*(2000000/n) for i in range(n)]
    start = int(n*0.25)
    p0,=axs.plot(x[start:], get_ops[start:], color=color[2], label='get', linestyle='solid')
    p2,=axs.plot(x[start:], put_ops[start:], color=color[2], label='put', linestyle='dashed')
    axs.set_xlabel('block number')
    axs.set_ylabel('number of ops')
    axs.legend()
    axs.set_title(title)

def plot_miss_ratio(axs, title):
    ficus_4_miss_ratio = 1-np.array(window_ave(ficus_4["ratio"], w=100))
    geth_4_miss_ratio = 1-np.array(window_ave(geth_4["ratio"], w=10))
    n = len(ficus_4_miss_ratio)
    x = [10000000+i*(2000000/n) for i in range(n)]
    start = int(n*0.25)
    p0,=axs.plot(x[start:], ficus_4_miss_ratio[start:], color=color[0], label='ficus-4g', linestyle='solid')
    p2,=axs.plot(x[start:], geth_4_miss_ratio[start:], color=color[1], label='geth-4g', linestyle='solid')
    axs.set_title(title)
    axs.yaxis.set_major_formatter(PercentFormatter(1))
    axs.set_xlabel('block number')
    axs.legend()

def eval_statedb(filename, w=1):
    fig, axs = plt.subplots(2, 3, figsize=(10.5, 6))

    #plot_cdf('a', axs[0][0])
    plot_block_time(axs[0][1], '(b) StateDB Runtime Per Block')
    plot_trpt(axs[0][2], '(c) StateDB Ops Throughput')
    plot_ops(axs[1][0], '(d) StateDB Ops Counter Per Block')
    plot_miss_ratio(axs[1][1], '(e) Cache Miss Ratio')
    # plot_breakdown('f', axs[1][2])

    fig.tight_layout()
    fig.savefig(filename, dpi=500)
    fig.clf()

eval_statedb('statedb-plot.png')