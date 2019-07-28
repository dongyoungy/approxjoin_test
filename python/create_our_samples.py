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


num_proc = 32

pool = mp.Pool(processes=num_proc, maxtasksperchild=10)
num_instacart_samples = 500
num_movielens_samples = 500
num_tpch100g_samples = 500
num_synthetic_samples = 500
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
impala_host = 'cp-4'
impala_port = 21050

# samples for instacart queries
args.append(
    (impala_host, impala_port, 'instacart', 'orders', 'order_id',
     'order_hour_of_day', 'instacart', 'order_products', 'order_id', None,
     'instacart_cent2', 'count', num_instacart_samples, overwrite))
args.append((impala_host, impala_port, 'instacart', 'orders', 'order_id',
             'days_since_prior', 'instacart', 'order_products', 'order_id',
             None, 'instacart_cent2', 'sum', num_instacart_samples, overwrite))
args.append((impala_host, impala_port, 'instacart', 'orders', 'order_id',
             'order_dow', 'instacart', 'order_products', 'order_id', None,
             'instacart_cent2', 'avg', num_instacart_samples, overwrite))

# samples for movielens queries
args.append((impala_host, impala_port, 'movielens', 'ratings', 'movieid',
             'rating', 'movielens', 'movies', 'movieid', None,
             'movielens_cent2', 'count', num_movielens_samples, overwrite))
args.append((impala_host, impala_port, 'movielens', 'ratings', 'movieid',
             'rating', 'movielens', 'movies', 'movieid', None,
             'movielens_cent2', 'sum', num_movielens_samples, overwrite))
args.append((impala_host, impala_port, 'movielens', 'ratings', 'movieid',
             'rating', 'movielens', 'movies', 'movieid', None,
             'movielens_cent2', 'avg', num_movielens_samples, overwrite))

# cent samples for synthetic
for leftDist in ['uniform_1', 'normal_1', 'powerlaw_1']:
    for rightDist in ['uniform_2', 'normal_2', 'powerlaw_2']:
        for agg in ['count', 'sum', 'avg']:
            args.append(
                (impala_host, impala_port, 'synthetic_10m', leftDist, 'col1',
                 'col2', 'synthetic_10m', rightDist, 'col1', None,
                 'synthetic_10m_cent2', agg, num_synthetic_samples, overwrite))

# dec samples for synthetic
dists = []
for leftDist in ['uniform_1', 'normal_1', 'powerlaw_1']:
    for rightDist in ['uniform_2', 'normal_2', 'powerlaw_2']:
        dists.append((leftDist, rightDist))

for dist in dists:
    for agg in ['count', 'sum', 'avg']:
        dec_args.append(
            ('cp-4', impala_port, 'synthetic_10m', dist[0], 'col1', 'col2',
             'synthetic_10m', dist[1], 'col1', None, 'synthetic_10m_dec2', agg,
             num_synthetic_samples, overwrite))

# dec samples for instacart queries
dec_args.append(
    (impala_host, impala_port, 'instacart', 'orders', 'order_id',
     'order_hour_of_day', 'instacart', 'order_products', 'order_id', None,
     'instacart_dec2', 'count', num_instacart_samples, overwrite))
dec_args.append(
    (impala_host, impala_port, 'instacart', 'orders', 'order_id',
     'days_since_prior', 'instacart', 'order_products', 'order_id', None,
     'instacart_dec2', 'sum', num_instacart_samples, overwrite))
dec_args.append((impala_host, impala_port, 'instacart', 'orders', 'order_id',
                 'order_dow', 'instacart', 'order_products', 'order_id', None,
                 'instacart_dec2', 'avg', num_instacart_samples, overwrite))

# create samples
for arg in args:
    results.append(
        pool.apply_async(sg.create_cent_sample_pair_from_impala,
                         arg,
                         callback=callback_success,
                         error_callback=callback_error))
for arg in dec_args:
    results.append(
        pool.apply_async(sg.create_dec_sample_pair_from_impala,
                         arg,
                         callback=callback_success,
                         error_callback=callback_error))

pool.join()

tpch_args = []
# samples for tpch100g queries
tpch_args.append(
    (impala_host, impala_port, 'tpch100g_parquet', 'lineitem', 'l_orderkey',
     'l_quantity', 'tpch100g_parquet', 'orders', 'o_orderkey', None,
     'tpch100g_cent2', 'count', num_tpch100g_samples, overwrite))
tpch_args.append(
    (impala_host, impala_port, 'tpch100g_parquet', 'lineitem', 'l_orderkey',
     'l_quantity', 'tpch100g_parquet', 'orders', 'o_orderkey', None,
     'tpch100g_cent2', 'sum', num_tpch100g_samples, overwrite))
tpch_args.append(
    (impala_host, impala_port, 'tpch100g_parquet', 'lineitem', 'l_orderkey',
     'l_extendedprice', 'tpch100g_parquet', 'orders', 'o_orderkey', None,
     'tpch100g_cent2', 'avg', num_tpch100g_samples, overwrite))

# create samples
for arg in tpch_args:
    results.append(
        pool.apply_async(sg.create_cent_sample_pair_from_impala,
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
