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

num_proc = 8

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
dists = []
dists.append(('uniform', 'uniform'))
dists.append(('uniform', 'normal'))
dists.append(('uniform', 'powerlaw'))
dists.append(('normal', 'normal'))
dists.append(('normal', 'powerlaw'))
dists.append(('powerlaw', 'powerlaw'))

aggs = ['count', 'sum', 'avg']
is_centralized_list = [True]

num_samples = 500

args = []
#  for num_row in [10 * 1000 * 1000]:
#  for num_key in [10 * 1000 * 1000]:
#  for dist in dists:
#  for agg in aggs:
#  for is_centralized in is_centralized_list:
#  args.append((num_row, num_key, num_row, num_key, dist[0],
#  dist[1], agg, num_samples, is_centralized))

dists = []
for leftDist in ['uniform', 'normal', 'powerlaw']:
    for rightDist in [
            'uniform', 'normal', 'normal1', 'normal2', 'powerlaw', 'powerlaw1',
            'powerlaw2'
    ]:
        dists.append((leftDist, rightDist))
dists.append(('uniform', 'uniform_max_var'))
dists.append(('normal', 'normal_max_var'))
dists.append(('powerlaw', 'powerlaw_max_var'))
aggs = ['count', 'sum']
for num_row in [10 * 1000 * 1000]:
    for num_key in [1 * 1000 * 1000]:
        for dist in dists:
            for agg in aggs:
                args.append((num_row, num_key, num_row, num_key, dist[0],
                             dist[1], agg, 3000, False))

preset_args = []
prob = []
prob.append((0.01, 1))
prob.append((0.015, 0.666))
prob.append((0.03, 0.333))
prob.append((0.333, 0.03))
prob.append((0.666, 0.015))
prob.append((1, 0.01))

# for centralized setting
dists = []
dists.append(('uniform', 'uniform'))
dists.append(('uniform', 'normal'))
dists.append(('uniform', 'powerlaw'))
dists.append(('normal', 'normal'))
dists.append(('normal', 'powerlaw'))
dists.append(('powerlaw', 'powerlaw'))

for num_row in [10 * 1000 * 1000]:
    for num_key in [1 * 1000 * 1000, 10 * 1000 * 1000]:
        for dist in dists:
            for p in prob:
                preset_args.append((num_row, num_key, dist[0], dist[1], p[0],
                                    p[1], num_samples))

results = []

#  for arg in preset_args:
#  results.append(
#  pool.apply_async(sg.create_preset_sample_pair,
#  arg,
#  callback=callback_success,
#  error_callback=callback_error))

for arg in args:
    results.append(
        pool.apply_async(sg.create_sample_pair,
                         arg,
                         callback=callback_success,
                         error_callback=callback_error))
#  for num_row in [10 * 1000 * 1000]:
#  for num_key in [10 * 1000 * 1000]:
#  for dist in dists:
#  for agg in aggs:
#  for is_centralized in is_centralized_list:
#  for s in range(1, num_samples + 1):
#  args.append((num_row, num_key, dist[0], dist[1], agg,
#  s, is_centralized))
#
#  pool = mp.Pool(8)
#  results = pool.starmap(cal.estimate_agg, args)

pool.close()
pool.join()

msg = MIMEText("N/A")
msg["From"] = "dyoon@umich.edu"
msg["To"] = "dyoon@umich.edu"
msg["Subject"] = "Run Successful"
p = Popen(["/usr/sbin/sendmail", "-t", "-oi"], stdin=PIPE)
p.communicate(msg.as_bytes())
