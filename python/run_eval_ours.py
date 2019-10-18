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

num_proc = 16

pool = mp.Pool(processes=num_proc, maxtasksperchild=10)
num_instacart_samples = 500
num_movielens_samples = 2000
num_tpch_samples = 500
num_synthetic_samples = 2000
overwrite = False
impala_host = "cp-10"
impala_port = 21050

synthetic_ours = []
synthetic_preset = []
instacart_ours = []
instacart_preset = []
movielens_ours = []
movielens_preset = []
tpch_ours = []
tpch_preset = []

if overwrite:
    conn = impaladb.connect(impala_host, impala_port)
    es.drop_result_table(conn, "synthetic_10m_cent_sample")
    es.drop_result_table(conn, "synthetic_10m_dec_sample")
    es.drop_result_table(conn, "synthetic_10m_preset")
    es.drop_result_table(conn, "instacart_cent_sample")
    es.drop_result_table(conn, "instacart_preset")
    es.drop_result_table(conn, "movielens_cent_sample")
    es.drop_result_table(conn, "movielens_preset")
    es.drop_result_table(conn, "tpch100g_cent_sample")
    es.drop_result_table(conn, "tpch100g_preset")


dists = []
dists.append(("uniform_1", "powerlaw3_2"))
dists.append(("normal_1", "powerlaw3_2"))
dists.append(("powerlaw3_1", "uniform_2"))
dists.append(("powerlaw3_1", "normal_2"))
dists.append(("powerlaw3_1", "powerlaw3_2"))
# for leftDist in ["powerlaw_1"]:
#     for rightDist in ["powerlaw_2"]:

# for d in dists:
#     leftDist = d[0]
#     rightDist = d[1]

# evaluate synthetic (cent)
# for leftDist in ["uniform_1", "normal_1", "powerlaw_1"]:
#     for rightDist in ["uniform_2", "normal_2", "powerlaw_2"]:
for leftDist in ["uniform_1", "normal_1"]:
    for rightDist in ["uniform_2", "normal_2"]:
        for agg in ["count", "sum", "avg"]:
            synthetic_ours.append(
                (
                    impala_host,
                    impala_port,
                    "synthetic_1m",
                    "synthetic_1m_cent1",
                    agg,
                    leftDist,
                    rightDist,
                    num_synthetic_samples,
                    False,
                )
            )

"""
# evaluate synthetic (dec)
for leftDist in ["uniform_1", "normal_1", "powerlaw_1"]:
    for rightDist in ["uniform_2", "normal_2", "powerlaw_2"]:
        for agg in ["count", "sum", "avg"]:
            synthetic_ours.append(
                (
                    impala_host,
                    impala_port,
                    "synthetic_10m",
                    "synthetic_10m_dec2",
                    agg,
                    leftDist,
                    rightDist,
                    num_synthetic_samples,
                    False,
                )
            )

# evaluate instacart (cent)
for agg in ["count", "sum", "avg"]:
    instacart_ours.append(
        (
            impala_host,
            impala_port,
            "instacart",
            "instacart_cent2",
            agg,
            num_instacart_samples,
            False,
        )
    )

# evaluate instacart (dec)
for agg in ["count", "sum", "avg"]:
    instacart_ours.append(
        (
            impala_host,
            impala_port,
            "instacart",
            "instacart_dec2",
            agg,
            num_instacart_samples,
            False,
        )
    )
"""


# evaluate movielens (ours)
"""
for agg in ["count", "sum", "avg"]:
    movielens_ours.append(
        (
            impala_host,
            impala_port,
            "movielens",
            "movielens_cent3_10p",
            agg,
            num_movielens_samples,
            False,
        )
    )
"""


"""
# evaluate tpch (ours)
for agg in ["count", "sum", "avg"]:
    tpch_ours.append(
        (
            impala_host,
            impala_port,
            "tpch1000g_parquet",
            "tpch1000g_cent1",
            agg,
            num_tpch_samples,
            False,
        )
    )
"""

# run
results = []
for arg in synthetic_ours:
    results.append(
        pool.apply_async(
            es.run_synthetic_ours,
            arg,
            callback=callback_success,
            error_callback=callback_error,
        )
    )
for arg in instacart_ours:
    results.append(
        pool.apply_async(
            es.run_instacart_ours,
            arg,
            callback=callback_success,
            error_callback=callback_error,
        )
    )
for arg in movielens_ours:
    results.append(
        pool.apply_async(
            es.run_movielens_ours,
            arg,
            callback=callback_success,
            error_callback=callback_error,
        )
    )
for arg in tpch_ours:
    results.append(
        pool.apply_async(
            es.run_tpch_ours,
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
