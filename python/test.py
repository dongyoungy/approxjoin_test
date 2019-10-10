import timeit
import sample_gen as sg
import data_gen as dg
import multiprocessing as mp
import calculate_agg as cal
import sys
from itertools import product

from email.mime.text import MIMEText
from subprocess import Popen, PIPE


def callback_error(result):
    print("Error: " + str(result.__cause__), file=sys.stderr, flush=True)
    msg = MIMEText("N/A")
    msg["From"] = "dyoon@umich.edu"
    msg["To"] = "dyoon@umich.edu"
    msg["Subject"] = "Run Failed."
    p = Popen(["/usr/sbin/sendmail", "-t", "-oi"], stdin=PIPE)
    p.communicate(msg.as_bytes())
    sys.exit("Terminate due to subprocess failure")


def callback_success(result):
    print("Success")


#  setup = "import sample_gen as sg"
#  sg.reset_schema()

#  for dist in ['uniform']:
#  for type in ['count', 'sum']:
#  for isCentralized in [True, False]:
#  s = "sg.create_sample(1000*1000*1000, 100*1000*1000, '{0}', '{0}', '{1}', {2})".format(
#  dist, type, isCentralized)
#  s = "sg.create_sample(100000, 10000, '{0}', '{0}', '{1}', {2})".format(
#  dist, type, isCentralized)
#  d = ''
#  if isCentralized:
#  d = 'cent'
#  else:
#  d = 'dec'
#  t = timeit.timeit(stmt=s, setup=setup, number=1)
#  print("[{0}, {1}, {2}] = {3} s".format(dist, type, d, t))

num_proc = 4

pool = mp.Pool(processes=num_proc, maxtasksperchild=10)
#  for k in [1000 * 1000, 10 * 1000 * 1000]:
#  for type in ['uniform', 'normal', 'powerlaw']:
#  for n in [1, 2]:
#  pool.apply_async(dg.create_table_data, [num_rows, k, type, n])
#  dg.create_table_data(num_rows, k, type, 2)

#  for k in [1000 * 1000, 10 * 1000 * 1000]:
#  for type in ['normal1', 'normal2', 'powerlaw1', 'powerlaw2']:
#  pool.apply_async(dg.create_table_data, [num_rows, k, type, 2])

#  for k in [1000 * 1000, 10 * 1000 * 1000]:
#  for type in ['uniform', 'normal', 'powerlaw']:
#  pool.apply_async(dg.create_max_var_table_data, [num_rows, k, type])

# for centralized setting
cen_dists = []
cen_dists.append(('uniform', 'uniform'))
cen_dists.append(('uniform', 'normal'))
cen_dists.append(('uniform', 'powerlaw'))
cen_dists.append(('normal', 'normal'))
cen_dists.append(('normal', 'powerlaw'))
cen_dists.append(('powerlaw', 'powerlaw'))

where_dists = []
where_dists.append(('uniform', 'uniform'))

aggs = ['count', 'sum', 'avg']
is_centralized_list = [True]

num_samples = 100

args = []
for num_row in [100 * 1000 * 1000]:
    for num_key in [1 * 1000 * 1000]:
        for dist in cen_dists:
            for agg in aggs:
                for is_centralized in is_centralized_list:
                    args.append((num_row, num_key, num_row, num_key, dist[0],
                                 dist[1], agg, num_samples, is_centralized))

# for decentralized setting
dists = []
for leftDist in ['uniform', 'normal', 'powerlaw']:
    #  for rightDist in [
    #  'uniform', 'normal', 'normal1', 'normal2', 'powerlaw', 'powerlaw1',
    #  'powerlaw2'
    #  ]:
    for rightDist in ['uniform', 'normal', 'powerlaw']:
        dists.append((leftDist, rightDist))
dists.append(('uniform', 'uniform_max_var'))
dists.append(('normal', 'normal_max_var'))
dists.append(('powerlaw', 'powerlaw_max_var'))
#  aggs = ['count', 'sum']
dec_aggs = ['count', 'sum', 'avg']
for num_row in [100 * 1000 * 1000]:
    for num_key in [1 * 1000 * 1000]:
        for dist in dists:
            for agg in dec_aggs:
                args.append((num_row, num_key, num_row, num_key, dist[0],
                             dist[1], agg, num_samples, False))

preset_args = []
prob = []
prob.append((0.01, 1))
prob.append((0.015, 0.666))
prob.append((0.03, 0.333))
prob.append((0.333, 0.03))
prob.append((0.666, 0.015))
prob.append((1, 0.01))

# for 2%
#  prob.append((0.02, 1))
#  prob.append((0.04, 0.5))
#  prob.append((0.08, 0.25))
#  prob.append((0.25, 0.08))
#  prob.append((0.5, 0.04))
#  prob.append((1, 0.02))

# for preset(baseline))
for num_row in [100 * 1000 * 1000]:
    for num_key in [1 * 1000 * 1000]:
        for dist in dists:
            for p in prob:
                preset_args.append((num_row, num_key, dist[0], dist[1], p[0],
                                    p[1], num_samples))

where_args = []
where_preset_args = []
#  rel_types = ['uniform', 'positive', 'negative']
rel_types = ['uniform', 'normal', 'powerlaw']
#rel_types = ['uniform']
types = ['uniform', 'identical']
#  types = ['uniform']
num_pred_val = 10
for num_row in [100 * 1000 * 1000]:
    for num_key in [1 * 1000 * 1000]:
        for dist in where_dists:
            for type in types:
                for rel_type in rel_types:
                    where_args.append(
                        (num_row, num_key, num_row, num_key, dist[0], dist[1],
                         type, rel_type, num_pred_val, num_samples))
for num_row in [100 * 1000 * 1000]:
    for num_key in [1 * 1000 * 1000]:
        for dist in where_dists:
            for p in prob:
                for rel_type in rel_types:
                    where_preset_args.append(
                        (num_row, num_key, dist[0], dist[1], p[0], p[1],
                         rel_type, num_samples))
results = []

for arg in args:
    results.append(
        pool.apply_async(sg.create_sample_pair,
                         arg,
                         callback=callback_success,
                         error_callback=callback_error))

for arg in preset_args:
    results.append(
        pool.apply_async(sg.create_preset_sample_pair,
                         arg,
                         callback=callback_success,
                         error_callback=callback_error))

for arg in where_args:
    results.append(
        pool.apply_async(sg.create_sample_pair_count_with_cond,
                         arg,
                         callback=callback_success,
                         error_callback=callback_error))

for arg in where_preset_args:
    results.append(
        pool.apply_async(sg.create_preset_sample_pair_with_cond,
                         arg,
                         callback=callback_success,
                         error_callback=callback_error))

pool.close()
pool.join()

msg = MIMEText("N/A")
msg["From"] = "dyoon@umich.edu"
msg["To"] = "dyoon@umich.edu"
msg["Subject"] = "Run Successful"
p = Popen(["/usr/sbin/sendmail", "-t", "-oi"], stdin=PIPE)
p.communicate(msg.as_bytes())
