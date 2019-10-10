import timeit
import evaluate_sample as es
import data_gen as dg
import multiprocessing as mp
import calculate_agg as cal
import sys
import impala.dbapi as impaladb
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


prob = []
prob.append((0.01, 1))
prob.append((0.015, 0.666))
prob.append((0.03, 0.333))
prob.append((0.333, 0.03))
prob.append((0.666, 0.015))
prob.append((1, 0.01))

num_proc = 10

pool = mp.Pool(processes=num_proc, maxtasksperchild=10)
num_instacart_samples = 100
num_synthetic_samples = 100
overwrite = False
impala_host = "cp-14"
impala_port = 21050

synthetic_ours = []
synthetic_preset = []
instacart_ours = []
instacart_preset = []

e1 = 0.03
e2 = 0.01

# evaluate synthetic (cent)
# for agg in ["count", "sum", "avg"]:
#     synthetic_ours.append(
#         (
#             impala_host,
#             impala_port,
#             "synthetic_10m",
#             "new_st_test2",
#             "normal_powerlaw2_1",
#             "normal_2",
#             agg,
#             e1,
#             e2,
#             10000,
#             100000,
#             num_synthetic_samples,
#             False,
#         )
#     )

# evaluate synthetic (preset)
for agg in ["count", "sum", "avg"]:
    synthetic_preset.append(
        (
            impala_host,
            impala_port,
            "synthetic_10m",
            "synthetic_10m_preset_strat5",
            "normal_powerlaw2_1",
            "normal_2",
            agg,
            100000,
            0.01,
            num_synthetic_samples,
            False,
        )
    )
    synthetic_preset.append(
        (
            impala_host,
            impala_port,
            "synthetic_10m",
            "synthetic_10m_preset_strat6",
            "normal_powerlaw2_1",
            "normal_2",
            agg,
            10000,
            0.01,
            num_synthetic_samples,
            False,
        )
    )


# run
results = []
for arg in synthetic_ours:
    results.append(
        pool.apply_async(
            es.run_synthetic_stratified_ours,
            arg,
            callback=callback_success,
            error_callback=callback_error,
        )
    )
for arg in synthetic_preset:
    results.append(
        pool.apply_async(
            es.run_synthetic_stratified_preset,
            arg,
            callback=callback_success,
            error_callback=callback_error,
        )
    )
# for arg in instacart_ours:
#     results.append(
#         pool.apply_async(
#             es.run_instacart_ours_with_where,
#             arg,
#             callback=callback_success,
#             error_callback=callback_error,
#         )
#     )
# for arg in instacart_preset:
#     results.append(
#         pool.apply_async(
#             es.run_instacart_preset_with_where,
#             arg,
#             callback=callback_success,
#             error_callback=callback_error,
#         )
#     )
pool.close()
pool.join()

msg = MIMEText("N/A")
msg["From"] = "dyoon@umich.edu"
msg["To"] = "dyoon@umich.edu"
msg["Subject"] = "Run Successful"
p = Popen(["/usr/sbin/sendmail", "-t", "-oi"], stdin=PIPE)
p.communicate(msg.as_bytes())
