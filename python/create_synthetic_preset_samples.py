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


num_proc = 16

pool = mp.Pool(processes=num_proc, maxtasksperchild=10)
num_samples = 100
overwrite = False
args = []
results = []

prob = []
prob.append((0.01, 1))
prob.append((0.015, 0.666))
prob.append((0.03, 0.333))
prob.append((0.333, 0.03))
prob.append((0.666, 0.015))
prob.append((1, 0.01))

T1_schema = 'synthetic_10m'
#  T1_table = 'orders'
T1_join_col = 'col1'
T2_schema = 'synthetic_10m'
#  T2_table = 'lineitem'
T2_join_col = 'col1'
target_schema = 'synthetic_10m_preset'
impala_host = 'cp-2'
impala_port = 21050

for p in prob:
    args.append((impala_host, impala_port, T1_schema, 'uniform_1', T1_join_col,
                 T2_schema, 'uniform_2', T2_join_col, target_schema, p[0],
                 p[1], num_samples, overwrite))
    args.append((impala_host, impala_port, T1_schema, 'uniform_1', T1_join_col,
                 T2_schema, 'normal_2', T2_join_col, target_schema, p[0], p[1],
                 num_samples, overwrite))
    args.append((impala_host, impala_port, T1_schema, 'uniform_1', T1_join_col,
                 T2_schema, 'powerlaw_2', T2_join_col, target_schema, p[0],
                 p[1], num_samples, overwrite))
    args.append((impala_host, impala_port, T1_schema, 'normal_1', T1_join_col,
                 T2_schema, 'uniform_2', T2_join_col, target_schema, p[0],
                 p[1], num_samples, overwrite))
    args.append((impala_host, impala_port, T1_schema, 'normal_1', T1_join_col,
                 T2_schema, 'normal_2', T2_join_col, target_schema, p[0], p[1],
                 num_samples, overwrite))
    args.append((impala_host, impala_port, T1_schema, 'normal_1', T1_join_col,
                 T2_schema, 'powerlaw_2', T2_join_col, target_schema, p[0],
                 p[1], num_samples, overwrite))
    args.append((impala_host, impala_port, T1_schema, 'powerlaw_1',
                 T1_join_col, T2_schema, 'uniform_2', T2_join_col,
                 target_schema, p[0], p[1], num_samples, overwrite))
    args.append((impala_host, impala_port, T1_schema, 'powerlaw_1',
                 T1_join_col, T2_schema, 'normal_2', T2_join_col,
                 target_schema, p[0], p[1], num_samples, overwrite))
    args.append((impala_host, impala_port, T1_schema, 'powerlaw_1',
                 T1_join_col, T2_schema, 'powerlaw_2', T2_join_col,
                 target_schema, p[0], p[1], num_samples, overwrite))
    args.append((impala_host, impala_port, T1_schema, 'uniform_1', T1_join_col,
                 T2_schema, 'uniform_max_var_2', T2_join_col, target_schema,
                 p[0], p[1], num_samples, overwrite))
    args.append((impala_host, impala_port, T1_schema, 'normal_1', T1_join_col,
                 T2_schema, 'normal_max_var_2', T2_join_col, target_schema,
                 p[0], p[1], num_samples, overwrite))
    args.append((impala_host, impala_port, T1_schema, 'powerlaw_1',
                 T1_join_col, T2_schema, 'powerlaw_max_var_2', T2_join_col,
                 target_schema, p[0], p[1], num_samples, overwrite))
    args.append((impala_host, impala_port, T1_schema, 'uniform_uniform_1',
                 T1_join_col, T2_schema, 'uniform_2', T2_join_col,
                 target_schema, p[0], p[1], num_samples, overwrite))
    args.append((impala_host, impala_port, T1_schema, 'uniform_normal_1',
                 T1_join_col, T2_schema, 'uniform_2', T2_join_col,
                 target_schema, p[0], p[1], num_samples, overwrite))
    args.append((impala_host, impala_port, T1_schema, 'uniform_powerlaw_1',
                 T1_join_col, T2_schema, 'uniform_2', T2_join_col,
                 target_schema, p[0], p[1], num_samples, overwrite))

for arg in args:
    results.append(
        pool.apply_async(sg.create_preset_sample_pair_from_impala,
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
