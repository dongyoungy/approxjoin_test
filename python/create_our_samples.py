import timeit
import multiprocessing as mp
import sys
from itertools import product

import calculate_agg as cal
import data_gen as dg
import sample_gen as sg
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
# num_instacart_samples = 500
# num_movielens_samples = 2000
# num_tpch_samples = 500
num_synthetic_samples = 2000
overwrite = False
dec_args = []
args = []
results = []

#  T1_schema = 'instacart'
#  T1_table = 'orders'
#  T1_join_col = 'order_id'
#  T2_schema = 'instacart'
#  T2_table = 'order_products'
#  T2_join_col = 'order_id'
#  target_schema = 'instacart_preset'
impala_host = "cp-9"
impala_port = 21050

# samples for instacart queries
"""
args.append(
    (
        impala_host,
        impala_port,
        "instacart",
        "orders",
        "order_id",
        "order_hour_of_day",
        "instacart",
        "order_products",
        "order_id",
        None,
        "instacart_cent2",
        "count",
        num_instacart_samples,
        overwrite,
    )
)
args.append(
    (
        impala_host,
        impala_port,
        "instacart",
        "orders",
        "order_id",
        "days_since_prior",
        "instacart",
        "order_products",
        "order_id",
        None,
        "instacart_cent2",
        "sum",
        num_instacart_samples,
        overwrite,
    )
)
args.append(
    (
        impala_host,
        impala_port,
        "instacart",
        "orders",
        "order_id",
        "order_dow",
        "instacart",
        "order_products",
        "order_id",
        None,
        "instacart_cent2",
        "avg",
        num_instacart_samples,
        overwrite,
    )
)
"""

# samples for movielens queries
"""
args.append(
    (
        impala_host,
        impala_port,
        "movielens",
        "ratings",
        "movieid",
        "rating",
        "movielens",
        "movies",
        "movieid",
        None,
        "movielens_cent3_10p",
        "count",
        num_movielens_samples,
        overwrite,
    )
)
args.append(
    (
        impala_host,
        impala_port,
        "movielens",
        "ratings",
        "movieid",
        "rating",
        "movielens",
        "movies",
        "movieid",
        None,
        "movielens_cent3_10p",
        "sum",
        num_movielens_samples,
        overwrite,
    )
)
args.append(
    (
        impala_host,
        impala_port,
        "movielens",
        "ratings",
        "movieid",
        "rating",
        "movielens",
        "movies",
        "movieid",
        None,
        "movielens_cent3_10p",
        "avg",
        num_movielens_samples,
        overwrite,
    )
)
"""

# cent samples for synthetic
for leftDist in ["t_1_uniform1_1", "t_3_normal1_1", "t_5_powerlaw1_1"]:
    for rightDist in ["t_1_uniform1_2", "t_3_normal1_2", "t_5_powerlaw1_2"]:
        for agg in ["count", "sum", "avg"]:
            args.append(
                (
                    impala_host,
                    impala_port,
                    "synthetic_100m",
                    leftDist,
                    "col1",
                    "col2",
                    "synthetic_100m",
                    rightDist,
                    "col1",
                    None,
                    "synthetic_100m_cent3",
                    agg,
                    num_synthetic_samples,
                    overwrite,
                )
            )

for leftDist in ["t_6_powerlaw2_1"]:
    for rightDist in ["t_2_uniform2_2", "t_4_normal2_2", "t_6_powerlaw2_2"]:
        for agg in ["count", "sum", "avg"]:
            args.append(
                (
                    impala_host,
                    impala_port,
                    "synthetic_100m",
                    leftDist,
                    "col1",
                    "col2",
                    "synthetic_100m",
                    rightDist,
                    "col1",
                    None,
                    "synthetic_100m_cent3",
                    agg,
                    num_synthetic_samples,
                    overwrite,
                )
            )

for leftDist in ["t_2_uniform2_1", "t_4_normal2_1"]:
    for rightDist in ["t_6_powerlaw2_2"]:
        for agg in ["count", "sum", "avg"]:
            args.append(
                (
                    impala_host,
                    impala_port,
                    "synthetic_100m",
                    leftDist,
                    "col1",
                    "col2",
                    "synthetic_100m",
                    rightDist,
                    "col1",
                    None,
                    "synthetic_100m_cent3",
                    agg,
                    num_synthetic_samples,
                    overwrite,
                )
            )

# for leftDist in ["uniform3_1", "normal3_1"]:
#     for rightDist in ["powerlaw3_2"]:
#         for agg in ["count", "sum", "avg"]:
#             args.append(
#                 (
#                     impala_host,
#                     impala_port,
#                     "synthetic_100m",
#                     leftDist,
#                     "col1",
#                     "col2",
#                     "synthetic_100m",
#                     rightDist,
#                     "col1",
#                     None,
#                     "synthetic_100m_cent2",
#                     agg,
#                     num_synthetic_samples,
#                     overwrite,
#                 )
#             )

# dec samples for synthetic
# dists = []
# for leftDist in ["uniform_1", "normal_1", "powerlaw_1"]:
#     for rightDist in ["uniform_2", "normal_2", "powerlaw_2"]:
#         dists.append((leftDist, rightDist))

# for dist in dists:
#     for agg in ["count", "sum", "avg"]:
#         dec_args.append(
#             (
#                 "cp-4",
#                 impala_port,
#                 "synthetic_10m",
#                 dist[0],
#                 "col1",
#                 "col2",
#                 "synthetic_10m",
#                 dist[1],
#                 "col1",
#                 None,
#                 "synthetic_10m_dec2",
#                 agg,
#                 num_synthetic_samples,
#                 overwrite,
#             )
#         )

# dec samples for instacart queries
# dec_args.append(
#     (
#         impala_host,
#         impala_port,
#         "instacart",
#         "orders",
#         "order_id",
#         "order_hour_of_day",
#         "instacart",
#         "order_products",
#         "order_id",
#         None,
#         "instacart_dec2",
#         "count",
#         num_instacart_samples,
#         overwrite,
#     )
# )
# dec_args.append(
#     (
#         impala_host,
#         impala_port,
#         "instacart",
#         "orders",
#         "order_id",
#         "days_since_prior",
#         "instacart",
#         "order_products",
#         "order_id",
#         None,
#         "instacart_dec2",
#         "sum",
#         num_instacart_samples,
#         overwrite,
#     )
# )
# dec_args.append(
#     (
#         impala_host,
#         impala_port,
#         "instacart",
#         "orders",
#         "order_id",
#         "order_dow",
#         "instacart",
#         "order_products",
#         "order_id",
#         None,
#         "instacart_dec2",
#         "avg",
#         num_instacart_samples,
#         overwrite,
#     )
# )

# create samples
for arg in args:
    results.append(
        pool.apply_async(
            sg.create_cent_sample_pair_from_impala_old,
            arg,
            callback=callback_success,
            error_callback=callback_error,
        )
    )
for arg in dec_args:
    results.append(
        pool.apply_async(
            sg.create_dec_sample_pair_from_impala,
            arg,
            callback=callback_success,
            error_callback=callback_error,
        )
    )

pool.close()
pool.join()

tpch_args = []
# samples for tpch300g queries
# tpch_args.append(
#     (
#         impala_host,
#         impala_port,
#         "tpch1000g_parquet",
#         "lineitem",
#         "l_orderkey",
#         "l_quantity",
#         "tpch1000g_parquet",
#         "orders",
#         "o_orderkey",
#         None,
#         "tpch1000g_cent1",
#         "count",
#         num_tpch_samples,
#         overwrite,
#     )
# )
# tpch_args.append(
#     (
#         impala_host,
#         impala_port,
#         "tpch1000g_parquet",
#         "lineitem",
#         "l_orderkey",
#         "l_quantity",
#         "tpch1000g_parquet",
#         "orders",
#         "o_orderkey",
#         None,
#         "tpch1000g_cent1",
#         "sum",
#         num_tpch_samples,
#         overwrite,
#     )
# )
# tpch_args.append(
#     (
#         impala_host,
#         impala_port,
#         "tpch1000g_parquet",
#         "lineitem",
#         "l_orderkey",
#         "l_extendedprice",
#         "tpch1000g_parquet",
#         "orders",
#         "o_orderkey",
#         None,
#         "tpch1000g_cent1",
#         "avg",
#         num_tpch_samples,
#         overwrite,
#     )
# )

pool = mp.Pool(processes=num_proc, maxtasksperchild=10)

# create samples
for arg in tpch_args:
    results.append(
        pool.apply_async(
            sg.create_cent_sample_pair_from_impala,
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
