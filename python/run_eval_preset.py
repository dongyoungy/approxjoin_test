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

# for 5%
# prob = []
# prob.append((0.05, 1))
# prob.append((0.075, 0.666))
# prob.append((0.15, 0.333))
# prob.append((0.333, 0.15))
# prob.append((0.666, 0.075))
# prob.append((1, 0.05))

# for 10%
# prob = []
# prob.append((0.1, 1))
# prob.append((0.15, 0.666))
# prob.append((0.3, 0.333))
# prob.append((0.333, 0.3))
# prob.append((0.666, 0.15))
# prob.append((1, 0.1))

num_proc = 16

pool = mp.Pool(processes=num_proc, maxtasksperchild=10)
num_instacart_samples = 500
num_movielens_samples = 2000
num_tpch_samples = 500
num_synthetic_samples = 2000
overwrite = False
impala_host = "cp-12"
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

idx = 1
# evaluate synthetic (preset)
# for leftDist in ["uniform1_1", "normal1_1", "powerlaw1_1"]:
#     for rightDist in ["uniform1_2", "normal1_2", "powerlaw1_2"]:
#         for agg in ["count", "sum", "avg"]:
# for leftDist in ["t_1_uniform1_1", "t_3_normal1_1", "t_5_powerlaw1_1"]:
#     for rightDist in ["t_1_uniform1_2", "t_3_normal1_2", "t_5_powerlaw1_2"]:
#         for agg in ["count", "sum", "avg"]:
#             for p in prob:
#                 synthetic_preset.append(
#                     (
#                         impala_host,
#                         impala_port,
#                         "synthetic_100m",
#                         "synthetic_100m_preset3",
#                         agg,
#                         leftDist,
#                         rightDist,
#                         p[0],
#                         p[1],
#                         num_synthetic_samples,
#                         False,
#                     )
#                 )

for agg in ["count", "sum", "avg"]:
    for p in prob:
        for leftDist in ["t_1_uniform1_1", "t_3_normal1_1", "t_5_powerlaw1_1"]:
            for rightDist in ["t_3_normal1_2", "t_5_powerlaw1_2"]:
                host = "cp-" + str(3 + (idx % 12))
                idx = idx + 1
                synthetic_preset.append(
                    (
                        host,
                        impala_port,
                        "synthetic_100m",
                        "synthetic_100m_preset3",
                        agg,
                        leftDist,
                        rightDist,
                        p[0],
                        p[1],
                        num_synthetic_samples,
                        False,
                    )
                )

for agg in ["count", "sum", "avg"]:
    for p in prob:
        for leftDist in ["t_3_normal1_1", "t_5_powerlaw1_1"]:
            for rightDist in ["t_1_uniform1_2", "t_3_normal1_2", "t_5_powerlaw1_2"]:
                host = "cp-" + str(3 + (idx % 12))
                idx = idx + 1
                synthetic_preset.append(
                    (
                        host,
                        impala_port,
                        "synthetic_100m",
                        "synthetic_100m_preset3",
                        agg,
                        leftDist,
                        rightDist,
                        p[0],
                        p[1],
                        num_synthetic_samples,
                        False,
                    )
                )

for agg in ["count", "sum", "avg"]:
    for p in prob:
        for leftDist in ["t_6_powerlaw2_1"]:
            for rightDist in ["t_2_uniform2_2", "t_4_normal2_2", "t_6_powerlaw2_2"]:
                host = "cp-" + str(3 + (idx % 12))
                idx = idx + 1
                synthetic_preset.append(
                    (
                        host,
                        impala_port,
                        "synthetic_100m",
                        "synthetic_100m_preset3",
                        agg,
                        leftDist,
                        rightDist,
                        p[0],
                        p[1],
                        num_synthetic_samples,
                        False,
                    )
                )

# for leftDist in ["uniform5_1", "normal5_1"]:
#     for rightDist in ["powerlaw5_2"]:
for agg in ["count", "sum", "avg"]:
    for p in prob:
        for leftDist in ["t_2_uniform2_1", "t_4_normal2_1"]:
            for rightDist in ["t_6_powerlaw2_2"]:
                host = "cp-" + str(3 + (idx % 12))
                idx = idx + 1
                synthetic_preset.append(
                    (
                        host,
                        impala_port,
                        "synthetic_100m",
                        "synthetic_100m_preset3",
                        agg,
                        leftDist,
                        rightDist,
                        p[0],
                        p[1],
                        num_synthetic_samples,
                        False,
                    )
                )
# for d in dists:
#     leftDist = d[0]
#     rightDist = d[1]
#     for agg in ["count", "sum", "avg"]:
#         for p in prob:
#             synthetic_preset.append(
#                 (
#                     impala_host,
#                     impala_port,
#                     "synthetic_10m",
#                     "synthetic_10m_preset2_2",
#                     agg,
#                     leftDist,
#                     rightDist,
#                     p[0],
#                     p[1],
#                     num_synthetic_samples,
#                     False,
#                 )
#             )

# # evaluate instacart (preset)
# for agg in ['count', 'sum', 'avg']:
#     for p in prob:
#         instacart_preset.append(
#             (impala_host, impala_port, 'instacart', 'instacart_preset2',
#              agg, p[0], p[1], num_instacart_samples, False))

# evaluate movielens (preset)
# for agg in ["count", "sum", "avg"]:
#     for p in prob:
#         movielens_preset.append(
#             (
#                 impala_host,
#                 impala_port,
#                 "movielens",
#                 "movielens_preset3_10p",
#                 agg,
#                 p[0],
#                 p[1],
#                 num_movielens_samples,
#                 False,
#             )
#         )

# evaluate tpch (preset)
# for agg in ["count", "sum", "avg"]:
#     for p in prob:
#         tpch_preset.append(
#             (
#                 impala_host,
#                 impala_port,
#                 "tpch1000g_parquet",
#                 "tpch1000g_preset2",
#                 agg,
#                 p[0],
#                 p[1],
#                 num_tpch_samples,
#                 False,
#             )
#         )

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
for arg in synthetic_preset:
    results.append(
        pool.apply_async(
            es.run_synthetic_preset,
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
for arg in instacart_preset:
    results.append(
        pool.apply_async(
            es.run_instacart_preset,
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
for arg in movielens_preset:
    results.append(
        pool.apply_async(
            es.run_movielens_preset,
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
for arg in tpch_preset:
    results.append(
        pool.apply_async(
            es.run_tpch_preset,
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
