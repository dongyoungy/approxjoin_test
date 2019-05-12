import datetime
import multiprocessing as mp
import pathlib
import sys
import time
from collections import defaultdict
from email.mime.text import MIMEText
from subprocess import PIPE, Popen

import numpy as np

import calculate_agg as cal


def callback_error(result):
    print("Error: " + str(result))
    msg = MIMEText("N/A")
    msg["From"] = "dyoon@umich.edu"
    msg["To"] = "dyoon@umich.edu"
    msg["Subject"] = "Run Failed."
    p = Popen(["/usr/sbin/sendmail", "-t", "-oi"], stdin=PIPE)
    p.communicate(msg.as_bytes())
    sys.exit("Terminate due to subprocess failure")


def callback_success(result):
    print("Success")


cen_result_path = '/home/dyoon/work/approxjoin_test/results/centralized'
dec_result_path = '/home/dyoon/work/approxjoin_test/results/decentralized'
preset_result_path = '/home/dyoon/work/approxjoin_test/results/preset'

pathlib.Path(cen_result_path).mkdir(parents=True, exist_ok=True)
pathlib.Path(dec_result_path).mkdir(parents=True, exist_ok=True)
pathlib.Path(preset_result_path).mkdir(parents=True, exist_ok=True)

ts = time.time()
time_str = datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d_%H%M%S')
num_proc = 16

pool = mp.Pool(processes=num_proc)

our_cen_results = defaultdict(list)
our_dec_results = defaultdict(list)
preset_results = defaultdict(list)

cen_dists = []

# for centralized setting
cen_dists.append(('uniform', 'uniform'))
cen_dists.append(('uniform', 'normal'))
cen_dists.append(('uniform', 'powerlaw'))
cen_dists.append(('normal', 'normal'))
cen_dists.append(('normal', 'powerlaw'))
cen_dists.append(('powerlaw', 'powerlaw'))

dec_dists = []
for leftDist in ['uniform', 'normal', 'powerlaw']:
    for rightDist in [
            'uniform', 'normal', 'normal1', 'normal2', 'powerlaw', 'powerlaw1',
            'powerlaw2'
    ]:
        dec_dists.append((leftDist, rightDist))
dec_dists.append(('uniform', 'uniform_max_var'))
dec_dists.append(('normal', 'normal_max_var'))
dec_dists.append(('powerlaw', 'powerlaw_max_var'))

cen_aggs = ['count', 'sum', 'avg']
dec_aggs = ['count', 'sum']

is_centralized_list = [True]

cen_num_samples = 500
dec_num_samples = 2000

cen_results = []
dec_results = []
cen_preset_results = []
dec_preset_results = []

cen_args = []
dec_args = []
cen_preset_args = []
dec_preset_args = []

prob = []
prob.append((0.01, 1))
prob.append((0.015, 0.666))
prob.append((0.03, 0.333))
prob.append((0.333, 0.03))
prob.append((0.666, 0.015))
prob.append((1, 0.01))

# centralized setting
'''
for num_row in [10 * 1000 * 1000]:
    for num_key in [10 * 1000 * 1000]:
        for dist in cen_dists:
            for agg in cen_aggs:
                for s in range(1, cen_num_samples + 1):
                    cen_args.append(
                        (num_row, num_key, dist[0], dist[1], agg, s, True))
'''

# centralized setting (preset)
for num_row in [10 * 1000 * 1000]:
    for num_key in [10 * 1000 * 1000]:
        for dist in cen_dists:
            for agg in cen_aggs:
                for p in prob:
                    for s in range(1, cen_num_samples + 1):
                        cen_preset_args.append((num_row, num_key, dist[0],
                                                dist[1], agg, p[0], p[1], s))

# decentralized setting
for num_row in [10 * 1000 * 1000]:
    for num_key in [1 * 1000 * 1000]:
        for dist in dec_dists:
            for agg in dec_aggs:
                for s in range(1, dec_num_samples + 1):
                    dec_args.append(
                        (num_row, num_key, dist[0], dist[1], agg, s, False))

# decentralized setting (preset)
for num_row in [10 * 1000 * 1000]:
    for num_key in [1 * 1000 * 1000]:
        for dist in dec_dists:
            for agg in dec_aggs:
                for p in prob:
                    for s in range(1, dec_num_samples + 1):
                        dec_preset_args.append((num_row, num_key, dist[0],
                                                dist[1], agg, p[0], p[1], s))

if cen_args:
    cen_results = pool.starmap(cal.estimate_agg, cen_args)
    for r in cen_results:
        # (num_rows, num_keys, left_dist, right_dist, agg_func, sample_idx)
        # => result
        our_cen_results[r[0:5]].append(r[6])
    if our_cen_results:
        result_file = "{}/our_cen_results_{}.npy".format(cen_result_path, time_str)
        np.save(result_file, our_cen_results)

if dec_args:
    dec_results = pool.starmap(cal.estimate_agg, dec_args)
    for r in dec_results:
        # (num_rows, num_keys, left_dist, right_dist, agg_func, sample_idx)
        # => result
        our_dec_results[r[0:5]].append(r[6])
    if our_dec_results:
        result_file = "{}/our_dec_results_{}.npy".format(dec_result_path, time_str)
        np.save(result_file, our_dec_results)

if cen_preset_args:
    cen_preset_results = pool.starmap(cal.estimate_preset_agg, cen_preset_args)
    for r in cen_preset_results:
        cen_preset_results[r[0:5]].append(r[6])
    if cen_preset_results:
        result_file = "{}/cen_preset_results_{}.npy".format(
            cen_result_path, time_str)
        np.save(result_file, cen_preset_results)

if dec_preset_args:
    dec_preset_results = pool.starmap(cal.estimate_preset_agg, dec_preset_args)
    for r in dec_preset_results:
        dec_preset_results[r[0:5]].append(r[6])
    if dec_preset_results:
        result_file = "{}/dec_preset_results_{}.npy".format(
            dec_result_path, time_str)
        np.save(result_file, dec_preset_results)

pool.close()
pool.join()

# notify me
msg = MIMEText("N/A")
msg["From"] = "dyoon@umich.edu"
msg["To"] = "dyoon@umich.edu"
msg["Subject"] = "Experiment Successful"
p = Popen(["/usr/sbin/sendmail", "-t", "-oi"], stdin=PIPE)
p.communicate(msg.as_bytes())
