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
num_instacart_samples = 1
num_synthetic_samples = 1
overwrite = False
dec_args = []
args = []
results = []

T1_schema = 'instacart'
T1_table = 'orders'
T1_join_col = 'order_id'
T2_schema = 'instacart'
T2_table = 'order_products'
T2_join_col = 'order_id'
target_schema = 'instacart_preset'
impala_host = 'cp-5'
impala_port = 21050

# samples for instacart queries
'''
args.append(
    (impala_host, impala_port, 'instacart', 'orders', 'order_id',
     'order_hour_of_day', 'instacart', 'order_products', 'order_id', None,
     'instacart_cent_sample', 'count', num_instacart_samples, overwrite))
args.append(
    (impala_host, impala_port, 'instacart', 'orders', 'order_id',
     'days_since_prior', 'instacart', 'order_products', 'order_id', None,
     'instacart_cent_sample', 'sum', num_instacart_samples, overwrite))
args.append((impala_host, impala_port, 'instacart', 'orders', 'order_id',
             'order_dow', 'instacart', 'order_products', 'order_id', None,
             'instacart_cent_sample', 'avg', num_instacart_samples, overwrite))
'''

# where samples for synthetic
for leftDist in [
        'uniform_uniform_1', 'uniform_normal_1', 'uniform_powerlaw_1'
]:
    for rightDist in ['uniform_2']:
        for agg in ['count', 'sum', 'avg']:
            for cond in ['eq', 'geq']:
                for where_type in ['uniform', 'identical']:
                    args.append(
                        (impala_host, impala_port, 'synthetic_10m', leftDist,
                         'col1', 'col2', 'synthetic_10m', rightDist, 'col1',
                         'col3', 'synthetic_10m_where_sample', agg, cond,
                         where_type, num_synthetic_samples, overwrite))

# where samples for instacart
for leftDist in ['orders']:
    for rightDist in ['order_products']:
        for agg in ['count', 'sum', 'avg']:
            for cond in ['eq', 'geq']:
                for where_type in ['uniform', 'identical']:
                    args.append(
                        (impala_host, impala_port, 'instacart', leftDist,
                         'order_id', 'days_since_prior', 'instacart',
                         rightDist, 'order_id', 'order_hour_of_day',
                         'instacart_where_sample', agg, cond, where_type,
                         num_synthetic_samples, overwrite))

# create samples
for arg in args:
    results.append(
        pool.apply_async(sg.create_cent_sample_pair_with_where_from_impala,
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
