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

num_proc = 32

pool = mp.Pool(processes=num_proc, maxtasksperchild=10)
num_instacart_samples = 500
num_synthetic_samples = 500
overwrite = False
impala_host = "cp-9"
impala_port = 21050

synthetic_ours = []
synthetic_preset = []
instacart_ours = []
instacart_preset = []

# evaluate synthetic (cent)
for leftDist in ["uniform_uniform_1", "uniform_normal_1", "uniform_powerlaw_1"]:
    for rightDist in ["uniform_2"]:
        for agg in ["count", "sum", "avg"]:
            for cond_type in ["eq"]:
                for where_dist in ["uniform", "identical"]:
                    synthetic_ours.append(
                        (
                            impala_host,
                            impala_port,
                            "synthetic_10m",
                            "synthetic_10m_where2",
                            agg,
                            leftDist,
                            rightDist,
                            cond_type,
                            where_dist,
                            num_synthetic_samples,
                            False,
                        )
                    )

# evaluate synthetic (preset)
for leftDist in ["uniform_uniform_1", "uniform_normal_1", "uniform_powerlaw_1"]:
    for rightDist in ["uniform_2"]:
        for agg in ["count", "sum", "avg"]:
            for cond_type in ["eq"]:
                for p in prob:
                    synthetic_preset.append(
                        (
                            impala_host,
                            impala_port,
                            "synthetic_10m",
                            "synthetic_10m_preset2",
                            agg,
                            leftDist,
                            rightDist,
                            cond_type,
                            p[0],
                            p[1],
                            num_synthetic_samples,
                            False,
                        )
                    )
# evaluate instacart (cent)
for agg in ["count", "sum", "avg"]:
    for cond_type in ["eq"]:
        for where_dist in ["uniform", "identical"]:
            instacart_ours.append(
                (
                    impala_host,
                    impala_port,
                    "instacart",
                    "instacart_where2",
                    agg,
                    cond_type,
                    where_dist,
                    num_instacart_samples,
                    False,
                )
            )
# evaluate instacart (preset)
for agg in ["count", "sum", "avg"]:
    for cond_type in ["eq"]:
        for p in prob:
            instacart_preset.append(
                (
                    impala_host,
                    impala_port,
                    "instacart",
                    "instacart_preset2",
                    agg,
                    cond_type,
                    p[0],
                    p[1],
                    num_instacart_samples,
                    False,
                )
            )

# run
results = []
for arg in synthetic_ours:
    results.append(
        pool.apply_async(
            es.run_synthetic_ours_with_where,
            arg,
            callback=callback_success,
            error_callback=callback_error,
        )
    )
for arg in synthetic_preset:
    results.append(
        pool.apply_async(
            es.run_synthetic_preset_with_where,
            arg,
            callback=callback_success,
            error_callback=callback_error,
        )
    )
for arg in instacart_ours:
    results.append(
        pool.apply_async(
            es.run_instacart_ours_with_where,
            arg,
            callback=callback_success,
            error_callback=callback_error,
        )
    )
for arg in instacart_preset:
    results.append(
        pool.apply_async(
            es.run_instacart_preset_with_where,
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
