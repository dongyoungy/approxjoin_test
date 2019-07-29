from scipy.optimize import fminbound

import gc
import os
import math
import time
import pathlib
import datetime
import hashlib
import uuid
import impala.dbapi as impaladb
import numpy as np
import pandas as pd
import threading

hdfs_dir = '/tmp/approxjoin'
data_path = '/home/dyoon/work/approxjoin_data/'
raw_data_path = '/media/hdd/approxjoin_test/synthetic/data'
text_schema = 'approxjoin_text'
parquet_schema = 'approxjoin_parquet'
impala_host = 'cp-2'
impala_port = 21050
fetch_size = 100000
e1 = 0.01
e2 = 0.01
m = 1540483477
modval = 2**32

def sigma1(v, a_v, b_v, q):
    sigma = ((1/q ** 2) - 1) * (a_v[v, 1] - 1) * (b_v[v, 1] - 1) + (1/q - 1) * (b_v[v, 1]-1) * (a_v[v, 2] - a_v[v, 1] + 1) + (1/q - 1) * (a_v[v, 1] - 1) * (b_v[v, 2] - b_v[v, 1] + 1)
    sigma[sigma<0] = 0


def two_level_sample_func(keys, a_v, b_v, q):
    sig = sigma1(keys, a_v, b_v, q)
    val = sum(math.sqrt((sig + a_v[keys, 2] * b_v[keys, 2]) * (2 + q * (a_v[keys, 1] + b_v[keys, 1] - 2) ))) ** 2

def create_two_level_sample_pair_from_impala(host,
                                        port,
                                        T1_schema,
                                        T1_table,
                                        T1_join_col,
                                        T2_schema,
                                        T2_table,
                                        T2_join_col,
                                        sample_schema,
                                        num_sample,
                                        overwrite=False,
                                        print_time=False):
    # seed the rng
    hash_val = int(
        hashlib.sha1(str(uuid.uuid1().bytes).encode()).hexdigest(), 16) % (10**
                                                                           8)
    np.random.seed(int(time.time()) + hash_val)

    conn = impaladb.connect(host, port)
    cur = conn.cursor()

    T1_join_key_count = 0
    T2_join_key_count = 0

    # get # of of join keys in each table
    T1_join_key_count_sql = "SELECT MAX({0}) FROM {1}.{2}".format(
        T1_join_col, T1_schema, T1_table)
    cur.execute(T1_join_key_count_sql)
    results = cur.fetchall()
    for row in results:
        T1_join_key_count = row[0]
    cur.close()

    cur = conn.cursor()
    T2_join_key_count_sql = "SELECT MAX({0}) FROM {1}.{2}".format(
        T2_join_col, T2_schema, T2_table)
    cur.execute(T2_join_key_count_sql)
    results = cur.fetchall()
    for row in results:
        T2_join_key_count = row[0]
    cur.close()

    num_keys = max([T1_join_key_count, T2_join_key_count])

    a_v = np.zeros((num_keys, 2))
    b_v = np.zeros((num_keys, 2))
    mu_v = np.zeros((num_keys, 2))
    var_v = np.zeros((num_keys, 2))

    keys = np.arange(1, num_keys + 1)
    #  a_v[:, 0] = keys
    #  b_v[:, 0] = keys
    #  mu_v[:, 0] = keys
    #  var_v[:, 0] = keys

    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS {0}".format(sample_schema))
    T1_where = " WHERE {}".format(T1_where) if T1_where else ""
    T1_group_by_count_sql = """SELECT {0}, COUNT(*) FROM {2}.{3} {4} GROUP BY {0};
    """.format(T1_join_col, 'N/A', T1_schema, T1_table, T1_where)
    cur.execute(T1_group_by_count_sql)

    while True:
        results = cur.fetchmany(fetch_size)
        if not results:
            break

        for row in results:
            col1 = row[0]
            cnt = row[1]
            mean = row[2]
            var = row[3]
            a_v[col1 - 1, 0] = cnt
            mu_v[col1 - 1, 0] = mean
            if var is None:
                var = 0
            var_v[col1 - 1, 0] = var
    a_v[:, 1] = a_v[:, 0]**2
    mu_v[:, 1] = mu_v[:, 0]**2

    p = 0

    cur = conn.cursor()
    T2_group_by_count_sql = """SELECT {0}, COUNT(*) FROM {1}.{2} GROUP BY {0};
    """.format(T2_join_col, T2_schema, T2_table)
    cur.execute(T2_group_by_count_sql)
    while True:
        results = cur.fetchmany(fetch_size)
        if not results:
            break

        for row in results:
            col1 = row[0]
            cnt = row[1]
            b_v[col1 - 1, 0] = cnt

    b_v[:, 1] = b_v[:, 0]**2
    fn = lambda q: two_level_sample_func(keys, a_v, b_v, q)
    q0 = 0.5
    q_min = fminbound(fn, 0, 1)
    sig = sigma1(keys, a_v, b_v, q_min)
    const1 = lambda C: sum(C * math.sqrt( (sig + a_v[:,2] * b_v[:,2]) / (2 + q_min * (a_v[:,1] + b_v[:,1] - 2)  )) * (2 + q_min * (a_v[:,1] + b_v[:,1] - 2)  )) - (100*1000*1000 * e1)

