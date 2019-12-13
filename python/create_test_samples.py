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
num_synthetic_samples = 2000
overwrite = False
dec_args = []
args = []
preset_args = []
results = []

impala_host = "cp-4"
impala_port = 21050

prob = []
# prob.append((0.01, 1))
# prob.append((0.015, 0.666))
# prob.append((0.03, 0.333))
# prob.append((0.333, 0.03))
# prob.append((0.666, 0.015))
# prob.append((1, 0.01))

# 0.1%
prob.append((0.001, 1))
prob.append((0.0015, 0.666))
prob.append((0.003, 0.333))
prob.append((0.333, 0.003))
prob.append((0.666, 0.0015))
prob.append((1, 0.001))

T1_schema = "synthetic_100m"
T1_join_col = "col1"
T2_schema = "synthetic_100m"
T2_join_col = "col1"
target_schema = "synthetic_100m_preset3"

# for leftDist in ["uniform1_1", "normal1_1", "powerlaw1_1"]:
#     for rightDist in ["uniform1_2", "normal1_2", "powerlaw1_2"]:
for leftDist in ["t_1_uniform1_1", "t_3_normal1_1", "t_5_powerlaw1_1"]:
    for rightDist in ["t_1_uniform1_2", "t_3_normal1_2", "t_5_powerlaw1_2"]:
        for p in prob:
            preset_args.append(
                (
                    impala_host,
                    impala_port,
                    T1_schema,
                    leftDist,
                    T1_join_col,
                    T2_schema,
                    rightDist,
                    T2_join_col,
                    target_schema,
                    p[0],
                    p[1],
                    num_synthetic_samples,
                    overwrite,
                )
            )

# for leftDist in ["powerlaw5_1"]:
#     for rightDist in ["uniform5_2", "normal5_2", "powerlaw5_2"]:
for leftDist in ["t_6_powerlaw2_1"]:
    for rightDist in ["t_2_uniform2_2", "t_4_normal2_2", "t_6_powerlaw2_2"]:
        for p in prob:
            preset_args.append(
                (
                    impala_host,
                    impala_port,
                    T1_schema,
                    leftDist,
                    T1_join_col,
                    T2_schema,
                    rightDist,
                    T2_join_col,
                    target_schema,
                    p[0],
                    p[1],
                    num_synthetic_samples,
                    overwrite,
                )
            )

# for leftDist in ["uniform5_1", "normal5_1"]:
#     for rightDist in ["powerlaw5_2"]:
for leftDist in ["t_2_uniform2_1", "t_4_normal2_1"]:
    for rightDist in ["t_6_powerlaw2_2"]:
        for p in prob:
            preset_args.append(
                (
                    impala_host,
                    impala_port,
                    T1_schema,
                    leftDist,
                    T1_join_col,
                    T2_schema,
                    rightDist,
                    T2_join_col,
                    target_schema,
                    p[0],
                    p[1],
                    num_synthetic_samples,
                    overwrite,
                )
            )
# create samples
for arg in args:
    results.append(
        pool.apply_async(
            sg.create_cent_sample_pair_from_impala,
            arg,
            callback=callback_success,
            error_callback=callback_error,
        )
    )
for arg in preset_args:
    results.append(
        pool.apply_async(
            sg.create_preset_sample_pair_from_impala,
            arg,
            callback=callback_success,
            error_callback=callback_error,
        )
    )
pool.close()
pool.join()

msg = MIMEText("N/A")
msg["From"] = "dyoon@umich.edu"
msg["To"] = "dyoon@umich.edu"
msg["Subject"] = "Run Successful"
p = Popen(["/usr/sbin/sendmail", "-t", "-oi"], stdin=PIPE)
p.communicate(msg.as_bytes())
