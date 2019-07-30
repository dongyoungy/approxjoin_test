import gc
import os
import math
import time
import pathlib
import datetime
# import MemDB
import hashlib
import uuid

import impala.dbapi as impaladb
import numpy as np
import pandas as pd
import threading

hdfs_dir = '/tmp/approxjoin'
data_path = '/home/dyoon/work/approxjoin_data/'
#  raw_data_path = '/home/dyoon/work/approxjoin_data/raw_data'
raw_data_path = '/media/hdd/approxjoin_test/synthetic/data'
text_schema = 'approxjoin_text'
parquet_schema = 'approxjoin_parquet'
#  sample_schema = 'approxjoin_parquet_sample'
impala_host = 'cp-2'
impala_port = 21050
fetch_size = 100000
e1 = 0.01
e2 = 0.01
m = 1540483477
modval = 2**32


def get_existing_tables(conn, schema):
    cur = conn.cursor()
    cur.execute("SHOW TABLES IN {}".format(schema))
    table_list = []
    results = cur.fetchall()
    for row in results:
        table_list.append(row[0])
    cur.close()
    return table_list


def estimate_variance(a_v, b_v, mu_v, var_v, p):
    e1 = 0.01
    e2 = 0.01
    q1 = e1 / p
    q2 = e2 / p
#  % calculate 'a' first
    a_common = sum(a_v[:,0] * mu_v[:,0] * b_v[:,0])**2
    a1 = ((1-q2) / (p*q2)) * sum(a_v[:,1] * mu_v[:,1] * b_v[:,0]) / a_common
    a2 = ((1-q1) / (p*q1)) * sum(a_v[:,0] * (mu_v[:,1] + var_v[:,0]) * b_v[:,1]) / a_common
    a3 = ((1-q1)*(1-q2) / (p*q1*q2)) * sum(a_v[:,0] * (mu_v[:,1] + var_v[:,0]) * b_v[:,0]) / a_common
    a4 = ((1-p)/p) * sum(a_v[:,1] * mu_v[:,1] * b_v[:,1]) / a_common
    a = a1 + a2 + a3 + a4;

#   % calculate 'b'
    b_common = sum(a_v[:,0] * mu_v[:,0] * b_v[:,0]) * sum(a_v[:,0] * b_v[:,0])
    b1 =  ((1-q2) / (p*q2))* sum(a_v[:,1] * mu_v[:,0] * b_v[:,0]) / b_common;
    b2 =  ((1-q1) / (p*q1)) * sum(a_v[:,0] * mu_v[:,0] * b_v[:,1]) / b_common
    b3 =  ((1-q1)*(1-q2) / (p*q1*q2)) * (1 / sum(a_v[:,0] * b_v[:,0]))
    b4 = ((1-p)/p) * sum(a_v[:,1] * mu_v[:,0] * b_v[:,1]) / b_common
    b = b1 + b2 + b3 + b4;

#   % calculate 'c'
    c_common = sum(a_v[:,0] * b_v[:,0])**2
    c1 = ((1-q2) / (p*q2)) * sum(a_v[:,1] * b_v[:,0]) / c_common;
    c2 =  ((1-q1) / (p*q1)) * sum(a_v[:,0] * b_v[:,1]) / c_common;
    c3 =  ((1-q1)*(1-q2) / (p*q1*q2)) * sum(a_v[:,0] * b_v[:,0]) / c_common;
    c4 = ((1-p)/p) * sum(a_v[:,1] * b_v[:,1]) / c_common;
    c = c1 + c2 + c3 + c4;

    e_sum = p * q1 * q2 * sum(a_v[:,0] * mu_v[:,0] * b_v[:,0])
    e_count = p * q1 * q2 * sum(a_v[:,0] *  b_v[:,0])

    estimate = (e_sum**2 / e_count**2) * (a + c)
    return estimate


def test1(num_rows, num_keys, leftDist, rightDist, type):
    T1_name = raw_data_path + "/t_{0}n_{1}k_{2}_{3}_{4}.csv".format(
        num_rows, num_keys, leftDist, type, 1)
    T2_name = raw_data_path + "/t_{0}n_{1}k_{2}_{3}.csv".format(
        num_rows, num_keys, rightDist, 2)

    # read table files
    T1_df = pd.read_csv(T1_name, sep='|', header=None, usecols=[0, 1, 2])
    T1 = T1_df.values
    T1 = T1.astype(int)
    T2_df = pd.read_csv(T2_name, sep='|', header=None, usecols=[0, 1, 2])
    T2 = T2_df.values
    T2 = T2.astype(int)

    a_v = np.zeros((num_keys, 3))
    b_v = np.zeros((num_keys, 3))
    mu_v = np.zeros((num_keys, 3))
    var_v = np.zeros((num_keys, 2))

    all_keys = np.arange(1, num_keys + 1)
    a_v[:, 0] = all_keys
    b_v[:, 0] = all_keys
    mu_v[:, 0] = all_keys
    var_v[:, 0] = all_keys

    # get group count for T1
    counts = np.array(np.unique(T1[:, 0], return_counts=True)).T
    a_v[counts[:, 0].astype(int) - 1, 1] = counts[:, 1]
    a_v[:, 2] = a_v[:, 1]**2

    # get group count for T2
    counts = np.array(np.unique(T2[:, 0], return_counts=True)).T
    b_v[counts[:, 0].astype(int) - 1, 1] = counts[:, 1]
    b_v[:, 2] = b_v[:, 1]**2

    # get mean and var
    gr = T1_df.groupby(0)
    keys = np.array(list(gr.groups.keys()))
    means = np.array(gr[1].mean().values)
    vars = np.array(np.nan_to_num(gr[1].var().values))

    mu_v[keys - 1, 1] = means
    mu_v[:, 2] = mu_v[:, 1]**2
    var_v[keys - 1, 1] = vars

    num_pred_val = 10

    a_v2 = np.zeros((num_keys, num_pred_val, 2))
    mu_v2 = np.zeros((num_keys, num_pred_val, 2))
    var_v2 = np.zeros((num_keys, num_pred_val, 1))

    # get mean and var
    gr = T1_df.groupby([0, 2])
    keys = np.array(list(gr.groups.keys()))
    counts = np.array(gr.count().values)
    means = np.array(gr[1].mean().values)
    vars = np.array(np.nan_to_num(gr[1].var().values))

    a_v2[keys[:, 0].astype(int) - 1, keys[:, 1].astype(int), 0] = counts[:, 0]
    a_v2[:, :, 1] = a_v2[:, :, 0]**2

    mu_v2[keys[:, 0].astype(int) - 1, keys[:, 1].astype(int), 0] = means
    mu_v2[:, :, 1] = mu_v2[:, :, 0]**2

    var_v2[keys[:, 0].astype(int) - 1, keys[:, 1].astype(int), 0] = vars

    sum1 = sum(a_v[:, 2] * b_v[:, 2] - a_v[:, 2] * b_v[:, 1] -
               a_v[:, 1] * b_v[:, 2] + a_v[:, 1] * b_v[:, 1])
    sum2 = sum(a_v[:, 1] * b_v[:, 1])
    val1 = math.sqrt(e1 * e2 * sum1 / sum2)

    for i in range(0, num_pred_val):
        a_temp = a_v2[:, i, :]
        sum1 += sum(a_temp[:, 1] * b_v[:, 2] - a_temp[:, 1] * b_v[:, 1] -
                    a_temp[:, 0] * b_v[:, 2] + a_temp[:, 0] * b_v[:, 1])
        sum2 += sum(a_temp[:, 0] * b_v[:, 1])
    val2 = math.sqrt(e1 * e2 * sum1 / sum2)

    return (val1, val2)


def reset_schema(sample_schema):
    conn = impaladb.connect(impala_host, impala_port)
    cur = conn.cursor()
    cur.execute("DROP SCHEMA IF EXISTS {0} CASCADE".format(sample_schema))
    cur.execute("CREATE SCHEMA IF NOT EXISTS {0}".format(sample_schema))


def estimate_with_sketch(host, port, schema, left_dist, right_dist):
    np.random.seed(int(time.time()))
    conn = impaladb.connect(host, port)
    cur = conn.cursor()

    #  T1_name = raw_data_path + "/t_{0}n_{1}k_{2}_{3}.csv".format(
    #  num_rows, num_keys, leftDist, 1)
    #  T2_name = raw_data_path + "/t_{0}n_{1}k_{2}_{3}.csv".format(
    #  num_rows, num_keys, rightDist, 2)

    # read table files
    #  T1_df = pd.read_csv(T1_name, sep='|', header=None, usecols=[0, 1, 2])
    #  T1 = T1_df.values
    #  T1 = T1.astype(int)
    #  T2_df = pd.read_csv(T2_name, sep='|', header=None, usecols=[0, 1, 2])
    #  T2 = T2_df.values
    #  T2 = T2.astype(int)

    # get # of of join keys in each table
    T1_join_key_count_sql = "SELECT MAX(col1) FROM {0}.{1}".format(
        schema, left_dist)
    cur.execute(T1_join_key_count_sql)
    results = cur.fetchall()
    for row in results:
        T1_join_key_count = row[0]
    cur.close()

    cur = conn.cursor()
    T2_join_key_count_sql = "SELECT MAX(col1) FROM {0}.{1}".format(
        schema, right_dist)
    cur.execute(T2_join_key_count_sql)
    results = cur.fetchall()
    for row in results:
        T2_join_key_count = row[0]
    cur.close()

    num_keys = max([T1_join_key_count, T2_join_key_count])

    a_v = np.zeros((num_keys, 3))
    b_v = np.zeros((num_keys, 3))
    mu_v = np.zeros((num_keys, 3))
    var_v = np.zeros((num_keys, 2))

    all_keys = np.arange(1, num_keys + 1)
    a_v[:, 0] = all_keys
    b_v[:, 0] = all_keys
    mu_v[:, 0] = all_keys
    var_v[:, 0] = all_keys

    cur = conn.cursor()
    T1_group_by_count_sql = """SELECT col1, COUNT(*), avg(col2), variance(col2) FROM {}.{} GROUP BY col1;
    """.format(schema, left_dist)
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
            a_v[col1 - 1, 1] = cnt
            mu_v[col1 - 1, 1] = mean
            if var is None:
                var = 0
            var_v[col1 - 1, 1] = var
    a_v[:, 2] = a_v[:, 1]**2
    mu_v[:, 2] = mu_v[:, 1]**2

    p = 0

    cur = conn.cursor()
    T2_group_by_count_sql = """SELECT col1, COUNT(*) FROM {}.{} GROUP BY col1;
    """.format(schema, right_dist)
    cur.execute(T2_group_by_count_sql)
    while True:
        results = cur.fetchmany(fetch_size)
        if not results:
            break

        for row in results:
            col1 = row[0]
            cnt = row[1]
            b_v[col1 - 1, 1] = cnt

    cur.close()
    b_v[:, 2] = b_v[:, 1]**2

    # get group count for T1
    #  counts = np.array(np.unique(T1[:, 0], return_counts=True)).T
    #  a_v[counts[:, 0].astype(int) - 1, 1] = counts[:, 1]
    #  a_v[:, 2] = a_v[:, 1]**2

    # get group count for T2
    #  counts = np.array(np.unique(T2[:, 0], return_counts=True)).T
    #  b_v[counts[:, 0].astype(int) - 1, 1] = counts[:, 1]
    #  b_v[:, 2] = b_v[:, 1]**2

    # get mean and var
    #  gr = T1_df.groupby(0)
    #  keys = np.array(list(gr.groups.keys()))
    #  means = np.array(gr[1].mean().values)
    #  vars = np.array(np.nan_to_num(gr[1].var().values))
    #
    #  mu_v[keys - 1, 1] = means
    #  mu_v[:, 2] = mu_v[:, 1]**2
    #  var_v[keys - 1, 1] = vars

    e1 = 0.01
    e2 = 0.01
    sum_val = sum(a_v[:, 1] * mu_v[:, 1] * b_v[:, 1])
    count_val = sum(a_v[:, 1] * b_v[:, 1])

    A_denom = sum(a_v[:, 1] * mu_v[:, 1] * b_v[:, 1])**2
    A1 = sum(a_v[:, 1] * (mu_v[:, 2] + var_v[:, 1]) * b_v[:, 1]) / A_denom
    A2 = sum(a_v[:, 2] * mu_v[:, 2] * b_v[:, 2]) / A_denom
    A3 = sum(a_v[:, 2] * mu_v[:, 2] * b_v[:, 1]) / A_denom
    A4 = sum(a_v[:, 1] * (mu_v[:, 2] + var_v[:, 1]) * b_v[:, 2]) / A_denom
    A = A1 + A2 - A3 - A4

    B_denom = sum(a_v[:, 1] * b_v[:, 1]) * sum(
        a_v[:, 1] * mu_v[:, 1] * b_v[:, 1])
    B1 = 1 / sum(a_v[:, 1] * b_v[:, 1])
    B2 = sum(a_v[:, 2] * mu_v[:, 1] * b_v[:, 2]) / B_denom
    B3 = sum(a_v[:, 2] * mu_v[:, 1] * b_v[:, 1]) / B_denom
    B4 = sum(a_v[:, 1] * mu_v[:, 1] * b_v[:, 2]) / B_denom
    B = B1 + B2 - B3 - B4

    C_denom = sum(a_v[:, 1] * b_v[:, 1])**2
    C1 = sum(a_v[:, 1] * b_v[:, 1]) / C_denom
    C2 = sum(a_v[:, 2] * b_v[:, 2]) / C_denom
    C3 = sum(a_v[:, 2] * b_v[:, 1]) / C_denom
    C4 = sum(a_v[:, 1] * b_v[:, 2]) / C_denom
    C = C1 + C2 - C3 - C4

    D = (1 / e1 * e2) * (A1 - (2 * B1) + C1)

    val1 = A - (2 * B) + C
    val2 = D

    if val1 <= 0 and val2 > 0:
        p = max(e1, e2)
    elif val1 > 0 and val2 <= 0:
        p = 1
    elif val1 > 0 and val2 > 0:
        val = (A - (2 * B) + C) / D
        if val > 0:
            val = math.sqrt(val)
        p = min([1, max([e1, e2, val])])
    else:
        p_minus = max(e1, e2)
        p_plus = 1
        pval1 = (1 / p_minus) * val1 + p_minus * val2
        pval2 = (1 / p_plus) * val1 + p_plus * val2
        if pval1 < pval2:
            p = p_minus
        else:
            p = p_plus

    # estimate using sketch
    num_sketch = 100
    v1 = 0  # sum(a_v[:,1] * mu_v[:,1] * b_v[:,1])
    v2 = 0  # sum(a_v[:,1] * (mu_v[:,2] + var_v[:,1]) * b_v[:,1])
    v3 = 0  # sum(a_v[:,2] * mu_v[:,2] * b_v[:,2])
    v4 = 0  # sum(a_v[:,2] * mu_v[:,2] * b_v[:,1])
    v5 = 0  # sum(a_v[:,1] * (mu_v[:,2] + var_v[:,1]) * b_v[:,2])
    v6 = 0  # sum(a_v[:,1] * b_v[:,1]))
    v7 = 0  # sum(a_v[:,2] * mu_v[:,1] * b_v[:,2])
    v8 = 0  # sum(a_v[:,2] * mu_v[:,1] * b_v[:,1])
    v9 = 0  # sum(a_v[:,1] * mu_v[:,1] * b_v[:,2])
    v10 = 0  # sum(a_v[:,2] * b_v[:,2]))
    v11 = 0  # sum(a_v[:,2] * b_v[:,1]))
    v12 = 0  # sum(a_v[:,1] * b_v[:,2]))
    for i in range(1, num_sketch + 1):
        print("current_sketch = {}".format(i))
        x = np.random.randint(0, 2, size=num_keys)
        x[x == 0] = -1
        v1_1 = sum(a_v[:, 1] * mu_v[:, 1] * x[:])
        v1_2 = sum(b_v[:, 1] * x[:])
        v1 = v1 + (v1_1 * v1_2)
        v2_1 = sum(a_v[:, 1] * (mu_v[:, 2] + var_v[:, 1]) * x[:])
        v2_2 = v1_2
        v2 = v2 + (v2_1 * v1_2)
        v3_1 = sum(a_v[:, 2] * mu_v[:, 2] * x[:])
        v3_2 = sum(b_v[:, 2] * x[:])
        v3 = v3 + (v3_1 * v3_2)
        v4_1 = v3_1
        v4_2 = v1_2
        v4 = v4 + (v4_1 * v4_2)
        v5_1 = v2_1
        v5_2 = v3_2
        v5 = v5 + (v5_1 * v5_2)
        v6_1 = sum(a_v[:, 1] * x[:])
        v6_2 = v1_2
        v6 = v6 + (v6_1 * v6_2)
        v7_1 = sum(a_v[:, 2] * mu_v[:, 1] * x[:])
        v7_2 = v3_2
        v7 = v7 + (v7_1 * v7_2)
        v8_1 = v7_1
        v8_2 = v1_2
        v8 = v8 + (v8_1 * v8_2)
        v9_1 = v1_1
        v9_2 = v3_2
        v9 = v9 + (v9_1 * v9_2)
        v10_1 = sum(a_v[:, 2] * x[:])
        v10_2 = v3_2
        v10 = v10 + (v10_1 * v10_2)
        v11_1 = v10_1
        v11_2 = v1_2
        v11 = v11 + (v11_1 * v11_2)
        v12_1 = v6_1
        v12_2 = v3_2
        v12 = v12 + (v12_1 * v12_2)

        v1a = v1 / i
        v2a = v2 / i
        v3a = v3 / i
        v4a = v4 / i
        v5a = v5 / i
        v6a = v6 / i
        v7a = v7 / i
        v8a = v8 / i
        v9a = v9 / i
        v10a = v10 / i
        v11a = v11 / i
        v12a = v12 / i

        print((i, count_val, sum_val, v1a, v6a))

    v1 = v1 / num_sketch
    v2 = v2 / num_sketch
    v3 = v3 / num_sketch
    v4 = v4 / num_sketch
    v5 = v5 / num_sketch
    v6 = v6 / num_sketch
    v7 = v7 / num_sketch
    v8 = v8 / num_sketch
    v9 = v9 / num_sketch
    v10 = v10 / num_sketch
    v11 = v11 / num_sketch
    v12 = v12 / num_sketch

    A_denom = v1**2
    A1 = v2 / A_denom
    A2 = v3 / A_denom
    A3 = v4 / A_denom
    A4 = v5 / A_denom
    A = A1 + A2 - A3 - A4

    B_denom = v6 * v1
    B1 = 1 / v6
    B2 = v7 / B_denom
    B3 = v8 / B_denom
    B4 = v9 / B_denom
    B = B1 + B2 - B3 - B4

    C_denom = v6**2
    C1 = v6 / C_denom
    C2 = v10 / C_denom
    C3 = v11 / C_denom
    C4 = v12 / C_denom
    C = C1 + C2 - C3 - C4

    D = (1 / e1 * e2) * (A1 - (2 * B1) + C1)

    val1_estimate = A - (2 * B) + C
    val2_estimate = D

    if val1_estimate <= 0 and val2_estimate > 0:
        p_estimate = max(e1, e2)
    elif val1_estimate > 0 and val2_estimate <= 0:
        p_estimate = 1
    elif val1_estimate > 0 and val2_estimate > 0:
        val_estimate = (A - (2 * B) + C) / D
        if val_estimate > 0:
            val_estimate = math.sqrt(val_estimate)
        p_estimate = min([1, max([e1, e2, val_estimate])])
    else:
        p_minus = max(e1, e2)
        p_plus = 1
        pval1 = (1 / p_minus) * val1_estimate + p_minus * val2_estimate
        pval2 = (1 / p_plus) * val1_estimate + p_plus * val2_estimate
        if pval1 < pval2:
            p_estimate = p_minus
        else:
            p_estimate = p_plus

    return (val1, val2, val1_estimate, val2_estimate, p, p_estimate)


def create_preset_sample_pair_with_cond(num_rows, num_keys, leftDist,
                                        rightDist, p, q, rel_type, num_sample):
    np.random.seed(int(time.time() + threading.get_ident()) % (2**32))
    sample_dir = data_path + 'preset_samples/with_cond'
    pathlib.Path(sample_dir).mkdir(parents=True, exist_ok=True)

    T1_name = raw_data_path + "/t_{0}n_{1}k_{2}_{3}_{4}.csv".format(
        num_rows, num_keys, leftDist, rel_type, 1)
    T2_name = raw_data_path + "/t_{0}n_{1}k_{2}_{3}.csv".format(
        num_rows, num_keys, rightDist, 2)

    # read table files
    T1_df = pd.read_csv(T1_name, sep='|', header=None, usecols=[0, 1, 2])
    # drop dummy col
    #  T1_df = T1_df.drop(columns=[3])
    T1 = T1_df.values
    T1 = T1.astype(int)

    T2_df = pd.read_csv(T2_name, sep='|', header=None, usecols=[0, 1, 2])
    #  T2_df = T2_df.drop(columns=[3])
    T2 = T2_df.values
    T2 = T2.astype(int)

    e1 = 0.01
    e2 = 0.01

    q1 = e1 / p
    q2 = e2 / p

    dir = "{}/{}n_{}k/{}_{}/{}/{:.3f}_{:.3f}/".format(sample_dir, num_rows,
                                                      num_keys, leftDist,
                                                      rightDist, rel_type, p,
                                                      q)
    pathlib.Path(dir).mkdir(parents=True, exist_ok=True)

    T1_extra = np.zeros((num_rows, 2))
    T2_extra = np.zeros((num_rows, 2))

    T1_new = np.hstack((T1, T1_extra))
    T2_new = np.hstack((T2, T2_extra))

    print("Starting to write {} samples @ {} in: {}".format(
        num_sample, str(datetime.datetime.now()), dir),
          flush=True)

    start = time.time()

    for s in range(1, num_sample + 1):

        S1_name = "{}/s1_{}.npy".format(dir, s)
        S2_name = "{}/s2_{}.npy".format(dir, s)
        if os.path.exists(S1_name) and os.path.exists(S2_name):
            #  print("Samples (#{}) already exist".format(s))
            continue

        # generate random key permutation
        key_perm = np.random.permutation(num_keys * 10) + 1
        hash_col_idx = 3

        T1_perm = key_perm[T1[:, 0] - 1]
        T2_perm = key_perm[T2[:, 0] - 1]

        T1_new[:, hash_col_idx] = T1_perm
        T2_new[:, hash_col_idx] = T2_perm

        S1 = T1_new[np.where(T1_new[:, hash_col_idx] %
                             (num_keys + 1) <= (p * num_keys))]
        S2 = T2_new[np.where(T2_new[:, hash_col_idx] %
                             (num_keys + 1) <= (p * num_keys))]

        S1_rows = np.size(S1, 0)
        S2_rows = np.size(S2, 0)

        prob_col_idx = 4
        S1[:, prob_col_idx] = np.random.rand(S1_rows)
        S2[:, prob_col_idx] = np.random.rand(S2_rows)

        S1 = S1[np.where(S1[:, prob_col_idx] <= q1)]
        S2 = S2[np.where(S2[:, prob_col_idx] <= q2)]

        S1 = S1[:, 0:3]
        S2 = S2[:, 0:3]

        S1_data = {}
        S1_data['sample'] = S1
        S1_data['p'] = p
        S1_data['q'] = q

        S2_data = {}
        S2_data['sample'] = S2
        S2_data['p'] = p
        S2_data['q'] = q

        np.save(S1_name, S1_data)
        np.save(S2_name, S2_data)

        del S1, S2, key_perm, T1_perm, T2_perm
        gc.collect()

    end = time.time()
    print("Sample creation done (took {} s) in: {}".format(
        str(end - start), dir),
          flush=True)
    return True


def create_preset_sample_pair(num_rows, num_keys, leftDist, rightDist, p, q,
                              num_sample):
    np.random.seed(int(time.time() + threading.get_ident()) % (2**32))
    sample_dir = data_path + 'preset_samples'
    pathlib.Path(sample_dir).mkdir(parents=True, exist_ok=True)

    T1_name = raw_data_path + "/t_{0}n_{1}k_{2}_{3}.csv".format(
        num_rows, num_keys, leftDist, 1)
    T2_name = raw_data_path + "/t_{0}n_{1}k_{2}_{3}.csv".format(
        num_rows, num_keys, rightDist, 2)

    # read table files
    T1_df = pd.read_csv(T1_name, sep='|', header=None, usecols=[0, 1, 2])
    # drop dummy col
    #  T1_df = T1_df.drop(columns=[3])
    T1 = T1_df.values

    T2_df = pd.read_csv(T2_name, sep='|', header=None, usecols=[0, 1, 2])
    #  T2_df = T2_df.drop(columns=[3])
    T2 = T2_df.values

    q1 = q
    q2 = q

    print("creating samples for p = {:.3f}, q = {:.3f}".format(p, q))

    dir = "{}/{}n_{}k/{}_{}/{:.3f}_{:.3f}/".format(sample_dir, num_rows,
                                                   num_keys, leftDist,
                                                   rightDist, p, q)
    pathlib.Path(dir).mkdir(parents=True, exist_ok=True)

    T1_extra = np.zeros((num_rows, 2))
    T2_extra = np.zeros((num_rows, 2))

    T1_new = np.hstack((T1, T1_extra))
    T2_new = np.hstack((T2, T2_extra))

    print("Starting to write {} samples @ {} in: {}".format(
        num_sample, str(datetime.datetime.now()), dir),
          flush=True)

    start = time.time()

    for s in range(1, num_sample + 1):

        S1_name = "{}/s1_{}.npy".format(dir, s)
        S2_name = "{}/s2_{}.npy".format(dir, s)
        if os.path.exists(S1_name) and os.path.exists(S2_name):
            #  print("Samples (#{}) already exist".format(s))
            continue

        # generate random key permutation
        key_perm = np.random.permutation(num_keys * 10) + 1
        hash_col_idx = 3

        T1_perm = key_perm[T1[:, 0] - 1]
        T2_perm = key_perm[T2[:, 0] - 1]

        T1_new[:, hash_col_idx] = T1_perm
        T2_new[:, hash_col_idx] = T2_perm

        S1 = T1_new[np.where(T1_new[:, hash_col_idx] % 100000 <= (p * 100000))]
        S2 = T2_new[np.where(T2_new[:, hash_col_idx] % 100000 <= (p * 100000))]

        S1_rows = np.size(S1, 0)
        S2_rows = np.size(S2, 0)

        prob_col_idx = 4
        S1[:, prob_col_idx] = np.random.rand(S1_rows)
        S2[:, prob_col_idx] = np.random.rand(S2_rows)

        S1 = S1[np.where(S1[:, prob_col_idx] <= q1)]
        S2 = S2[np.where(S2[:, prob_col_idx] <= q2)]

        S1 = S1[:, 0:3]
        S2 = S2[:, 0:3]

        S1_data = {}
        S1_data['sample'] = S1
        S1_data['p'] = p
        S1_data['q'] = q

        S2_data = {}
        S2_data['sample'] = S2
        S2_data['p'] = p
        S2_data['q'] = q

        np.save(S1_name, S1_data)
        np.save(S2_name, S2_data)

        del S1, S2, key_perm, T1_perm, T2_perm
        gc.collect()

    end = time.time()
    print("Sample creation done (took {} s) in: {}".format(
        str(end - start), dir),
          flush=True)
    return True


def create_sample_pair(T1_rows,
                       T1_keys,
                       T2_rows,
                       T2_keys,
                       leftDist,
                       rightDist,
                       type,
                       num_samples,
                       isCentralized=True):
    # seed the rng
    hash_val = int(hashlib.sha1(type.encode()).hexdigest(), 16) % (10**8)
    np.random.seed(
        (int(time.time()) + hash_val + threading.get_ident()) % (2**32))
    dir = data_path + 'our_samples/'
    d = ''
    if isCentralized:
        d = 'centralized'
    else:
        d = 'decentralized'

    sample_dir = dir + d + '/'
    pathlib.Path(sample_dir).mkdir(parents=True, exist_ok=True)

    T1_name = raw_data_path + "/t_{0}n_{1}k_{2}_{3}.csv".format(
        T1_rows, T1_keys, leftDist, 1)
    T2_name = raw_data_path + "/t_{0}n_{1}k_{2}_{3}.csv".format(
        T2_rows, T2_keys, rightDist, 2)

    num_keys = max([T1_keys, T2_keys])

    # read table files
    T1_df = pd.read_csv(T1_name, sep='|', header=None, usecols=[0, 1, 2])
    T1 = T1_df.values
    T1 = T1.astype(int)
    T2_df = pd.read_csv(T2_name, sep='|', header=None, usecols=[0, 1, 2])
    T2 = T2_df.values
    T2 = T2.astype(int)

    # read table files
    #  T1_df = pd.read_csv(T1_name, sep='|', header=None, usecols=[0, 1, 2])
    # drop dummy col
    #  T1_df = T1_df.drop(columns=[3])
    #  T1 = T1_df.values

    #  T2_df = pd.read_csv(T2_name, sep='|', header=None, usecols=[0, 1, 2])
    #  T2_df = T2_df.drop(columns=[3])
    #  T2 = T2_df.values

    a_v = np.zeros((num_keys, 3))
    b_v = np.zeros((num_keys, 3))
    mu_v = np.zeros((num_keys, 3))
    var_v = np.zeros((num_keys, 2))

    all_keys = np.arange(1, num_keys + 1)
    a_v[:, 0] = all_keys
    b_v[:, 0] = all_keys
    mu_v[:, 0] = all_keys
    var_v[:, 0] = all_keys

    # get group count for T1
    counts = np.array(np.unique(T1[:, 0], return_counts=True)).T
    a_v[counts[:, 0].astype(int) - 1, 1] = counts[:, 1]
    a_v[:, 2] = a_v[:, 1]**2

    # get mean and var
    gr = T1_df.groupby(0)
    keys = np.array(list(gr.groups.keys()))
    means = np.array(gr[1].mean().values)
    vars = np.array(np.nan_to_num(gr[1].var().values))

    mu_v[keys - 1, 1] = means
    mu_v[:, 2] = mu_v[:, 1]**2
    var_v[keys - 1, 1] = vars

    if isCentralized:
        # get group count for T2
        counts = np.array(np.unique(T2[:, 0], return_counts=True)).T
        b_v[counts[:, 0].astype(int) - 1, 1] = counts[:, 1]
        b_v[:, 2] = b_v[:, 1]**2

        if type == 'count':
            sum1 = sum(a_v[:, 2] * b_v[:, 2] - a_v[:, 2] * b_v[:, 1] -
                       a_v[:, 1] * b_v[:, 2] + a_v[:, 1] * b_v[:, 1])
            sum2 = sum(a_v[:, 1] * b_v[:, 1])
            val = math.sqrt(e1 * e2 * sum1 / sum2)
            p = min([1, max([e1, e2, val])])
        elif type == 'sum':
            sum1 = sum(a_v[:, 2] * mu_v[:, 2] * b_v[:, 2])
            sum2 = sum(a_v[:, 2] * mu_v[:, 2] * b_v[:, 1])
            sum3 = sum(a_v[:, 1] * (mu_v[:, 2] + var_v[:, 1]) * b_v[:, 2])
            sum4 = sum(a_v[:, 1] * (mu_v[:, 2] + var_v[:, 1]) * b_v[:, 1])
            sum5 = sum(a_v[:, 1] * (mu_v[:, 2] + var_v[:, 1]) * b_v[:, 1])
            val = e1 * e2 * (sum1 - sum2 - sum3 + sum4) / sum5
            if val > 0:
                val = math.sqrt(val)
            p = min([1, max([e1, e2, val])])
        elif type == 'avg':
            A_denom = sum(a_v[:, 1] * mu_v[:, 1] * b_v[:, 1])**2
            A1 = sum(a_v[:, 1] *
                     (mu_v[:, 2] + var_v[:, 1]) * b_v[:, 1]) / A_denom
            A2 = sum(a_v[:, 2] * mu_v[:, 2] * b_v[:, 2]) / A_denom
            A3 = sum(a_v[:, 2] * mu_v[:, 2] * b_v[:, 1]) / A_denom
            A4 = sum(a_v[:, 1] *
                     (mu_v[:, 2] + var_v[:, 1]) * b_v[:, 2]) / A_denom
            A = A1 + A2 - A3 - A4

            B_denom = sum(a_v[:, 1] * b_v[:, 1]) * sum(
                a_v[:, 1] * mu_v[:, 1] * b_v[:, 1])
            B1 = 1 / sum(a_v[:, 1] * b_v[:, 1])
            B2 = sum(a_v[:, 2] * mu_v[:, 1] * b_v[:, 2]) / B_denom
            B3 = sum(a_v[:, 2] * mu_v[:, 1] * b_v[:, 1]) / B_denom
            B4 = sum(a_v[:, 1] * mu_v[:, 1] * b_v[:, 2]) / B_denom
            #  B3 = sum(a_v[:, 1] * mu_v[:, 1] * b_v[:, 2]) * sum(
            #  a_v[:, 1] * mu_v[:, 1] * b_v[:, 1]) / B_denom
            #  B3 = sum(a_v[:, 2] * mu_v[:, 1] * b_v[:, 1]) * sum(
            #  a_v[:, 1] * mu_v[:, 1] * b_v[:, 1]) / B_denom
            #  B4 = sum(a_v[:, 2] * mu_v[:, 2] * b_v[:, 1]) * sum(
            #  a_v[:, 1] * mu_v[:, 1] * b_v[:, 1]) / B_denom
            B = B1 + B2 - B3 - B4

            C_denom = sum(a_v[:, 1] * b_v[:, 1])**2
            C1 = sum(a_v[:, 1] * b_v[:, 1]) / C_denom
            C2 = sum(a_v[:, 2] * b_v[:, 2]) / C_denom
            C3 = sum(a_v[:, 2] * b_v[:, 1]) / C_denom
            C4 = sum(a_v[:, 1] * b_v[:, 2]) / C_denom
            #  C2 = sum(a_v[:, 2] * b_v[:, 1]) * sum(
            #  a_v[:, 1] * mu_v[:, 1] * b_v[:, 1])**2 / C_denom
            #  C3 = sum(a_v[:, 1] * b_v[:, 2]) * sum(
            #  a_v[:, 1] * mu_v[:, 1] * b_v[:, 1])**2 / C_denom
            #  C4 = sum(a_v[:, 2] * b_v[:, 2]) * sum(
            #  a_v[:, 1] * mu_v[:, 1] * b_v[:, 1])**2 / C_denom
            C = C1 + C2 - C3 - C4

            D = (1 / e1 * e2) * (A1 - (2 * B1) + C1)

            val1 = A - (2 * B) + C
            val2 = D

            # old implementation
            #  if val1 <= 0 and val2 > 0:
            #  p = max(e1, e2)
            #  elif val1 > 0 and val2 <= 0:
            #  p = 1
            #  elif val1 > 0 and val2 > 0:
            #  val = (A - (2 * B) + C) / D
            #  if val > 0:
            #  val = math.sqrt(val)
            #  p = min([1, max([e1, e2, val])])
            #  else:
            #  p = min([1, max([e1, e2, val])])
            if val1 <= 0 and val2 > 0:
                p = max(e1, e2)
            elif val1 > 0 and val2 <= 0:
                p = 1
            elif val1 > 0 and val2 > 0:
                val = (A - (2 * B) + C) / D
                if val > 0:
                    val = math.sqrt(val)
                p = min([1, max([e1, e2, val])])
            else:
                p_minus = max(e1, e2)
                p_plus = 1
                pval1 = (1 / p_minus) * val1 + p_minus * val2
                pval2 = (1 / p_plus) * val1 + p_plus * val2
                if pval1 < pval2:
                    p = p_minus
                else:
                    p = p_plus
    else:
        a_star = max(a_v[:, 1])
        if type == 'count':
            # get group count for T2
            counts = np.array(np.unique(T2[:, 0], return_counts=True)).T
            b_v[counts[:, 0].astype(int) - 1, 1] = counts[:, 1]
            b_v[:, 2] = b_v[:, 1]**2
            b_star = max(b_v[:, 1])
            # new formulation
            val = math.sqrt(e1 * e2 * (a_star * b_star - a_star - b_star + 1))
            p = min([1, max([e1, e2, val])])
        elif type == 'sum':
            n_b = T2_rows
            v = np.zeros((num_keys, 3))
            v[:, 0] = all_keys
            v[:, 1] = a_v[:, 2] * mu_v[:, 2]
            v[:, 2] = a_v[:, 1] * (mu_v[:, 2] + var_v[:, 1])
            v1 = np.argmax(v[:, 1])
            v2 = np.argmax(v[:, 2])
            a_vi = [a_v[v1, 1], a_v[v2, 1]]
            mu_vi = [mu_v[v1, 1], mu_v[v2, 1]]
            var_vi = [var_v[v1, 1], var_v[v2, 1]]

            #  quadratic equation for h1(p) - h2(p) = 0
            eq = [
                h_p2(e1, e2, a_vi[0], n_b, mu_vi[0], var_vi[0]) -
                h_p2(e1, e2, a_vi[1], n_b, mu_vi[1], var_vi[1]),
                h_p(e1, e2, a_vi[0], n_b, mu_vi[0], var_vi[0]) -
                h_p(e1, e2, a_vi[1], n_b, mu_vi[1], var_vi[1]),
                h_const(e1, e2, a_vi[0], n_b, mu_vi[0], var_vi[0]) -
                h_const(e1, e2, a_vi[1], n_b, mu_vi[1], var_vi[1])
            ]
            r = np.roots(eq)
            p1 = 0
            p2 = 0
            if len(r) == 2:
                p1 = r[0]
                p2 = r[1]

            sum1 = a_v[v1, 2] * mu_v[v1, 2] * n_b**2
            sum2 = a_v[v1, 2] * mu_v[v1, 2] * n_b
            sum3 = a_v[v1, 1] * (mu_v[v1, 2] + var_v[v1, 1]) * n_b**2
            sum4 = a_v[v1, 1] * (mu_v[v1, 2] + var_v[v1, 1]) * n_b
            sum5 = a_v[v1, 1] * (mu_v[v1, 2] + var_v[v1, 1]) * n_b
            val = e1 * e2 * (sum1 - sum2 - sum3 + sum4) / sum5
            if val > 0:
                val = math.sqrt(val)
            p3 = min([1, max([e1, e2, val])])

            sum1 = a_v[v2, 2] * mu_v[v2, 2] * n_b**2
            sum2 = a_v[v2, 2] * mu_v[v2, 2] * n_b
            sum3 = a_v[v2, 1] * (mu_v[v2, 2] + var_v[v2, 1]) * n_b**2
            sum4 = a_v[v2, 1] * (mu_v[v2, 2] + var_v[v2, 1]) * n_b
            sum5 = a_v[v2, 1] * (mu_v[v2, 2] + var_v[v2, 1]) * n_b
            val = e1 * e2 * (sum1 - sum2 - sum3 + sum4) / sum5
            if val > 0:
                val = math.sqrt(val)
            p4 = min([1, max([e1, e2, val])])

            p5 = max([e1, e2])

            pval = np.zeros((5, 2))
            pval[0, 0] = p1
            pval[1, 0] = p2
            pval[2, 0] = p3
            pval[3, 0] = p4
            pval[4, 0] = p5
            pval[0, 1] = max([
                h(p1, e1, e2, a_vi[0], n_b, mu_vi[0], var_vi[0]),
                h(p1, e1, e2, a_vi[1], n_b, mu_vi[1], var_vi[1])
            ]) if p1 != 0 else 0
            pval[1, 1] = max([
                h(p2, e1, e2, a_vi[0], n_b, mu_vi[0], var_vi[0]),
                h(p2, e1, e2, a_vi[1], n_b, mu_vi[1], var_vi[1])
            ]) if p2 != 0 else 0
            pval[2, 1] = max([
                h(p3, e1, e2, a_vi[0], n_b, mu_vi[0], var_vi[0]),
                h(p3, e1, e2, a_vi[1], n_b, mu_vi[1], var_vi[1])
            ]) if p3 != 0 else 0
            pval[3, 1] = max([
                h(p4, e1, e2, a_vi[0], n_b, mu_vi[0], var_vi[0]),
                h(p4, e1, e2, a_vi[1], n_b, mu_vi[1], var_vi[1])
            ]) if p4 != 0 else 0
            pval[4, 1] = max([
                h(p5, e1, e2, a_vi[0], n_b, mu_vi[0], var_vi[0]),
                h(p5, e1, e2, a_vi[1], n_b, mu_vi[1], var_vi[1])
            ]) if p5 != 0 else 0
            check_real = np.isreal(pval[:, 0])
            pval = np.delete(pval, np.argwhere(check_real is False), 0)
            pval = np.delete(pval, np.argwhere(pval[:, 0] < max([e1, e2])), 0)
            m = np.argmin(pval[:, 1])
            p = pval[m, 0]
        elif type == 'avg':
            # get group count for T2
            counts = np.array(np.unique(T2[:, 0], return_counts=True)).T
            b_v[counts[:, 0].astype(int) - 1, 1] = counts[:, 1]
            b_v[:, 2] = b_v[:, 1]**2

            # estimate using sketch
            num_sketch = 1000
            v1 = 0  # sum(a_v[:,1] * mu_v[:,1] * b_v[:,1])
            v2 = 0  # sum(a_v[:,1] * (mu_v[:,2] + var_v[:,1]) * b_v[:,1])
            v3 = 0  # sum(a_v[:,2] * mu_v[:,2] * b_v[:,2])
            v4 = 0  # sum(a_v[:,2] * mu_v[:,2] * b_v[:,1])
            v5 = 0  # sum(a_v[:,1] * (mu_v[:,2] + var_v[:,1]) * b_v[:,2])
            v6 = 0  # sum(a_v[:,1] * b_v[:,1]))
            v7 = 0  # sum(a_v[:,2] * mu_v[:,1] * b_v[:,2])
            v8 = 0  # sum(a_v[:,2] * mu_v[:,1] * b_v[:,1])
            v9 = 0  # sum(a_v[:,1] * mu_v[:,1] * b_v[:,2])
            v10 = 0  # sum(a_v[:,2] * b_v[:,2]))
            v11 = 0  # sum(a_v[:,2] * b_v[:,1]))
            v12 = 0  # sum(a_v[:,1] * b_v[:,2]))
            for i in range(0, num_sketch):
                print("current_sketch = {}".format(i))
                x = np.random.randint(0, 2, size=num_keys)
                x[x == 0] = -1
                v1_1 = sum(a_v[:, 1] * mu_v[:, 1] * x[:])
                v1_2 = sum(b_v[:, 1] * x[:])
                v1 = v1 + (v1_1 * v1_2)
                v2_1 = sum(a_v[:, 1] * (mu_v[:, 2] + var_v[:, 1]) * x[:])
                v2_2 = v1_2
                v2 = v2 + (v2_1 * v1_2)
                v3_1 = sum(a_v[:, 2] * mu_v[:, 2] * x[:])
                v3_2 = sum(b_v[:, 2] * x[:])
                v3 = v3 + (v3_1 * v3_2)
                v4_1 = v3_1
                v4_2 = v1_2
                v4 = v4 + (v4_1 * v4_2)
                v5_1 = v2_1
                v5_2 = v3_2
                v5 = v5 + (v5_1 * v5_2)
                v6_1 = sum(a_v[:, 1] * x[:])
                v6_2 = v1_2
                v6 = v6 + (v6_1 * v6_2)
                v7_1 = sum(a_v[:, 2] * mu_v[:, 1] * x[:])
                v7_2 = v3_2
                v7 = v7 + (v7_1 * v7_2)
                v8_1 = v7_1
                v8_2 = v1_2
                v8 = v8 + (v8_1 * v8_2)
                v9_1 = v1_1
                v9_2 = v3_2
                v9 = v9 + (v9_1 * v9_2)
                v10_1 = sum(a_v[:, 2] * x[:])
                v10_2 = v3_2
                v10 = v10 + (v10_1 * v10_2)
                v11_1 = v10_1
                v11_2 = v1_2
                v11 = v11 + (v11_1 * v11_2)
                v12_1 = v6_1
                v12_2 = v3_2
                v12 = v12 + (v12_1 * v12_2)

            v1 = v1 / num_sketch
            v2 = v2 / num_sketch
            v3 = v3 / num_sketch
            v4 = v4 / num_sketch
            v5 = v5 / num_sketch
            v6 = v6 / num_sketch
            v7 = v7 / num_sketch
            v8 = v8 / num_sketch
            v9 = v9 / num_sketch
            v10 = v10 / num_sketch
            v11 = v11 / num_sketch
            v12 = v12 / num_sketch

            A_denom = v1**2
            A1 = v2 / A_denom
            A2 = v3 / A_denom
            A3 = v4 / A_denom
            A4 = v5 / A_denom
            A = A1 + A2 - A3 - A4

            B_denom = v6 * v1
            B1 = 1 / v6
            B2 = v7 / B_denom
            B3 = v8 / B_denom
            B4 = v9 / B_denom
            B = B1 + B2 - B3 - B4

            C_denom = v6**2
            C1 = v6 / C_denom
            C2 = v10 / C_denom
            C3 = v11 / C_denom
            C4 = v12 / C_denom
            C = C1 + C2 - C3 - C4

            D = (1 / e1 * e2) * (A1 - (2 * B1) + C1)

            val1 = A - (2 * B) + C
            val2 = D

            if val1 <= 0 and val2 > 0:
                p = max(e1, e2)
            elif val1 > 0 and val2 <= 0:
                p = 1
            elif val1 > 0 and val2 > 0:
                val = (A - (2 * B) + C) / D
                if val > 0:
                    val = math.sqrt(val)
                p = min([1, max([e1, e2, val])])
            else:
                p_minus = max(e1, e2)
                p_plus = 1
                pval1 = (1 / p_minus) * val1 + p_minus * val2
                pval2 = (1 / p_plus) * val1 + p_plus * val2
                if pval1 < pval2:
                    p = p_minus
                else:
                    p = p_plus
        else:
            print('Unsupported type: {}'.format(type))
            raise ValueError

    if p != 0.01:
        p = 0.01
    q1 = e1 / p
    q2 = e2 / p

    dir = "{}/{}n_{}k/{}_{}/{}".format(sample_dir, T1_rows, T1_keys, leftDist,
                                       rightDist, type)
    pathlib.Path(dir).mkdir(parents=True, exist_ok=True)

    print("{}: p = {:.3f}".format(dir, p))

    # clear resources
    del a_v, b_v, mu_v, var_v, gr, keys, means, vars
    gc.collect()

    T1_extra = np.zeros((T1_rows, 2))
    T2_extra = np.zeros((T2_rows, 2))

    T1_new = np.hstack((T1, T1_extra))
    T2_new = np.hstack((T2, T2_extra))

    print("Starting to write {} samples @ {} in: {}".format(
        num_samples, str(datetime.datetime.now()), dir),
          flush=True)

    start = time.time()

    for s in range(1, num_samples + 1):

        S1_name = "{}/s1_{}.npy".format(dir, s)
        S2_name = "{}/s2_{}.npy".format(dir, s)
        #  if os.path.exists(S1_name) and os.path.exists(S2_name):
        #  print("Samples (#{}) already exist".format(s))
        #  continue

        # generate random key permutation
        key_perm = np.random.permutation(num_keys) + 1
        hash_col_idx = 3

        T1_perm = key_perm[T1[:, 0] - 1]
        #  T1_perm.shape = (T1_rows, 1)

        T2_perm = key_perm[T2[:, 0] - 1]
        #  T2_perm.shape = (T2_rows, 1)

        #  T1_new = np.hstack((T1, T1_perm))
        #  T2_new = np.hstack((T2, T2_perm))
        T1_new[:, hash_col_idx] = T1_perm
        T2_new[:, hash_col_idx] = T2_perm

        S1 = T1_new[np.where(T1_new[:, hash_col_idx] % 100000 <= (p * 100000))]
        S2 = T2_new[np.where(T2_new[:, hash_col_idx] % 100000 <= (p * 100000))]

        S1_rows = np.size(S1, 0)
        S2_rows = np.size(S2, 0)

        prob_col_idx = 4
        #  S1 = np.hstack((S1, np.random.rand(S1_rows, 1)))
        #  S2 = np.hstack((S2, np.random.rand(S2_rows, 1)))
        S1[:, prob_col_idx] = np.random.rand(S1_rows)
        S2[:, prob_col_idx] = np.random.rand(S2_rows)

        S1 = S1[np.where(S1[:, prob_col_idx] <= q1)]
        S2 = S2[np.where(S2[:, prob_col_idx] <= q2)]

        S1 = S1[:, 0:3]
        S2 = S2[:, 0:3]

        S1_data = {}
        S1_data['sample'] = S1
        S1_data['p'] = p
        S1_data['q'] = q1

        S2_data = {}
        S2_data['sample'] = S2
        S2_data['p'] = p
        S2_data['q'] = q2

        np.save(S1_name, S1_data)
        np.save(S2_name, S2_data)

        del S1, S2, key_perm, T1_perm, T2_perm
        gc.collect()

    end = time.time()
    print("Sample creation done (took {} s) in: {}".format(
        str(end - start), dir),
          flush=True)
    return True


def create_preset_sample_pair_from_impala_test(host,
                                               port,
                                               T1_schema,
                                               T1_table,
                                               T1_join_col,
                                               T2_schema,
                                               T2_table,
                                               T2_join_col,
                                               target_schema,
                                               p,
                                               q,
                                               num_sample,
                                               overwrite=False):
    # seed the rng
    hash_val = int(
        hashlib.sha1(str(uuid.uuid1().bytes).encode()).hexdigest(), 16) % (10**
                                                                           8)
    np.random.seed(int(time.time()) + hash_val)
    conn = impaladb.connect(host, port)
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS {}".format(target_schema))

    tables = get_existing_tables(conn, target_schema)
    m = 1540483477
    modval = 2**32

    for i in range(1, num_sample + 1):
        hash_num = np.random.randint(1000 * 1000)
        hash_num2 = np.random.randint(1000 * 1000)
        #  hash_num2 = 2**32
        S1_name = "s__{}__{}__{:.3f}p__{:.3f}q_{}_{}".format(
            T1_table, T2_table, p, q, i, 1)
        S2_name = "s__{}__{}__{:.3f}p__{:.3f}q_{}_{}".format(
            T1_table, T2_table, p, q, i, 2)
        S1_name = S1_name.replace('.', '_')
        S2_name = S2_name.replace('.', '_')

        if overwrite:
            cur.execute("DROP TABLE IF EXISTS {}.{}".format(
                target_schema, S1_name))
            cur.execute("DROP TABLE IF EXISTS {}.{}".format(
                target_schema, S2_name))
            conn.commit()
        else:
            if S1_name in tables and S2_name in tables:
                continue

        create_S1_sql = """CREATE TABLE IF NOT EXISTS {0}.{1} STORED AS PARQUET AS SELECT * FROM (
        SELECT *, pmod(bitxor(bitxor({7} * {9}, rotateright({7} * {9}, 24)) * {9}, {6} * {9}), {10}) as pval
        FROM {2}.{3}
        ) tmp
        WHERE pval <= {4} * {10} and rand(unix_timestamp() + {6} + 1) <= {5}
        """.format(target_schema, S1_name, T1_schema, T1_table, p, q,
                   hash_num + i, T1_join_col, hash_num2, m, modval)
        create_S2_sql = """CREATE TABLE IF NOT EXISTS {0}.{1} STORED AS PARQUET AS SELECT * FROM (
        SELECT *, pmod(bitxor(bitxor({7} * {9}, rotateright({7} * {9}, 24)) * {9}, {6} * {9}), {10}) as pval
        FROM {2}.{3}
        ) tmp
        WHERE pval <= {4} * {10} and rand(unix_timestamp() + {6} + 1) <= {5}
        """.format(target_schema, S2_name, T2_schema, T2_table, p, q,
                   hash_num + i, T2_join_col, hash_num2, m, modval)

        #  create_S2_sql = """CREATE TABLE IF NOT EXISTS {0}.{1} STORED AS PARQUET AS SELECT * FROM (
        #  SELECT *, pmod(fnv_hash({7} + {6}), 1000*1000)+1 as pval
        #  FROM {2}.{3}
        #  ) tmp
        #  WHERE pval <= {4} * 1000*1000 and rand(unix_timestamp() + {6} + 2) <= {5}
        #  """.format(target_schema, S2_name, T2_schema, T2_table, p, q,
        #  hash_num + i, T2_join_col)

        cur.execute(create_S1_sql)
        cur.execute(create_S2_sql)
        cur.execute("COMPUTE STATS {}.{}".format(target_schema, S1_name))
        cur.execute("COMPUTE STATS {}.{}".format(target_schema, S2_name))
        #  cur.execute("SELECT COUNT(distinct col1) from {}.{}".format(
        #  target_schema, S1_name))
        #  res = cur.fetchone()
        #  print(res[0])


def create_preset_sample_pair_from_impala(host,
                                          port,
                                          T1_schema,
                                          T1_table,
                                          T1_join_col,
                                          T2_schema,
                                          T2_table,
                                          T2_join_col,
                                          target_schema,
                                          p,
                                          q,
                                          num_sample,
                                          overwrite=False):
    # seed the rng
    hash_val = int(
        hashlib.sha1(str(uuid.uuid1().bytes).encode()).hexdigest(), 16) % (10**
                                                                           8)
    np.random.seed(int(time.time()) + hash_val)
    conn = impaladb.connect(host, port)
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS {}".format(target_schema))

    tables = get_existing_tables(conn, target_schema)
    m = 1540483477
    modval = 2**32

    for i in range(1, num_sample + 1):
        hash_num = np.random.randint(1000 * 1000)
        hash_num2 = np.random.randint(1000 * 1000)
        S1_name = "s__{}__{}__{:.3f}p__{:.3f}q_{}_{}".format(
            T1_table, T2_table, p, q, i, 1)
        S2_name = "s__{}__{}__{:.3f}p__{:.3f}q_{}_{}".format(
            T1_table, T2_table, p, q, i, 2)
        S1_name = S1_name.replace('.', '_')
        S2_name = S2_name.replace('.', '_')

        if overwrite:
            cur.execute("DROP TABLE IF EXISTS {}.{}".format(
                target_schema, S1_name))
            cur.execute("DROP TABLE IF EXISTS {}.{}".format(
                target_schema, S2_name))
            conn.commit()
        else:
            if S1_name in tables and S2_name in tables:
                continue

        create_S1_sql = """CREATE TABLE IF NOT EXISTS {0}.{1} STORED AS PARQUET AS SELECT * FROM (
        SELECT *, pmod(bitxor(bitxor({7} * {9}, rotateright({7} * {9}, 24)) * {9}, {6} * {9}), {10}) as pval
        FROM {2}.{3}
        ) tmp
        WHERE pval <= {4} * {10} and rand(unix_timestamp() + {6} + 1) <= {5}
        """.format(target_schema, S1_name, T1_schema, T1_table, p, q,
                   hash_num + i, T1_join_col, hash_num2, m, modval)
        create_S2_sql = """CREATE TABLE IF NOT EXISTS {0}.{1} STORED AS PARQUET AS SELECT * FROM (
        SELECT *, pmod(bitxor(bitxor({7} * {9}, rotateright({7} * {9}, 24)) * {9}, {6} * {9}), {10}) as pval
        FROM {2}.{3}
        ) tmp
        WHERE pval <= {4} * {10} and rand(unix_timestamp() + {6} + 1) <= {5}
        """.format(target_schema, S2_name, T2_schema, T2_table, p, q,
                   hash_num + i, T2_join_col, hash_num2, m, modval)
        #  create_S1_sql = """CREATE TABLE IF NOT EXISTS {0}.{1} STORED AS PARQUET AS SELECT * FROM (
        #  SELECT *, pmod(fnv_hash({7} + {6}), 1000*1000)+1 as pval
        #  FROM {2}.{3}
        #  ) tmp
        #  WHERE pval <= {4} * 1000*1000 and rand(unix_timestamp() + {6} + 1) <= {5}
        #  """.format(target_schema, S1_name, T1_schema, T1_table, p, q,
        #  hash_num + i, T1_join_col)
        #
        #  create_S2_sql = """CREATE TABLE IF NOT EXISTS {0}.{1} STORED AS PARQUET AS SELECT * FROM (
        #  SELECT *, pmod(fnv_hash({7} + {6}), 1000*1000)+1 as pval
        #  FROM {2}.{3}
        #  ) tmp
        #  WHERE pval <= {4} * 1000*1000 and rand(unix_timestamp() + {6} + 2) <= {5}
        #  """.format(target_schema, S2_name, T2_schema, T2_table, p, q,
        #  hash_num + i, T2_join_col)

        cur.execute(create_S1_sql)
        cur.execute(create_S2_sql)
        #  cur.execute("COMPUTE STATS {}.{}".format(target_schema, S1_name))
        #  cur.execute("COMPUTE STATS {}.{}".format(target_schema, S2_name))


def create_dec_sample_pair_from_impala(host,
                                       port,
                                       T1_schema,
                                       T1_table,
                                       T1_join_col,
                                       T1_agg_col,
                                       T2_schema,
                                       T2_table,
                                       T2_join_col,
                                       T1_where,
                                       sample_schema,
                                       agg_type,
                                       num_sample,
                                       overwrite=False,
                                       print_time=False):

    t1 = datetime.datetime.now()
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

    if T1_table == 'uniform_1':
        worst_table = 'uniform_max_var_2'
    elif T1_table == 'normal_1':
        worst_table = 'normal_max_var_2'
    elif T1_table == 'powerlaw_1':
        worst_table = 'powerlaw_max_var_2'
    elif T1_table == 'orders':
        worst_table = 'order_products'

    cur = conn.cursor()
    T2_join_key_count_sql = "SELECT MAX({0}) FROM {1}.{2}".format(
        T2_join_col, T2_schema, worst_table)
    cur.execute(T2_join_key_count_sql)
    results = cur.fetchall()
    for row in results:
        T2_join_key_count = row[0]
    cur.close()

    num_keys = max([T1_join_key_count, T2_join_key_count])

    a_v = np.zeros((num_keys, 3))
    b_v = np.zeros((num_keys, 3))
    mu_v = np.zeros((num_keys, 3))
    var_v = np.zeros((num_keys, 2))

    keys = np.arange(1, num_keys + 1)
    a_v[:, 0] = keys
    b_v[:, 0] = keys
    mu_v[:, 0] = keys
    var_v[:, 0] = keys

    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS {0}".format(sample_schema))
    T1_where = " WHERE {}".format(T1_where) if T1_where else ""
    T1_group_by_count_sql = """SELECT {0}, COUNT(*), avg({1}), variance({1}) FROM {2}.{3} {4} GROUP BY {0};
    """.format(T1_join_col, T1_agg_col, T1_schema, T1_table, T1_where)
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
            a_v[col1 - 1, 1] = cnt
            mu_v[col1 - 1, 1] = mean
            if var is None:
                var = 0
            var_v[col1 - 1, 1] = var
    a_v[:, 2] = a_v[:, 1]**2
    mu_v[:, 2] = mu_v[:, 1]**2
    mu_v = np.nan_to_num(mu_v)
    var_v = np.nan_to_num(var_v)

    p = 0
    n_b = 0
    a_star = max(a_v[:, 1])

    if agg_type == 'count':
        cur = conn.cursor()
        T2_count_sql = """SELECT COUNT(*) FROM {}.{}""".format(
            T2_schema, worst_table)
        #  T2_max_group_by_count_sql = """SELECT max(cnt) FROM (SELECT {0}, COUNT(*) as cnt FROM {1}.{2} GROUP BY {0}) tmp;
        #  """.format(T2_join_col, T2_schema, T2_table)
        cur.execute(T2_count_sql)
        results = cur.fetchall()
        for row in results:
            b_star = int(row[0] * 0.75)
        t2 = datetime.datetime.now()
        #  sum1 = e1 * e2 * (a_star^2 * n_b^2 + a_star^2 * n_b + a_star * n_b^2 + a_star * n_b);
        #  sum2 = a_star * n_b;
        #  sum1 = e1 * e2 * (a_star**2 * n_b**2 + a_star**2 * n_b +
        #  a_star * n_b**2 + a_star * n_b)
        #  sum2 = a_star * n_b
        #  val = math.sqrt(sum1 / sum2)
        #  p = min([1, max([e1, e2, val])])
        # new formulation
        val = math.sqrt(e1 * e2 * (a_star * b_star - a_star - b_star + 1))
        p = min([1, max([e1, e2, val])])
    elif agg_type == 'sum':
        cur = conn.cursor()
        T2_table_count_sql = """SELECT COUNT(*) FROM {0}.{1};
        """.format(T2_schema, worst_table)
        cur.execute(T2_table_count_sql)
        results = cur.fetchall()
        for row in results:
            n_b = row[0]
        v = np.zeros((num_keys, 3))

        v[:, 0] = keys
        #  v(:,2) = a_v(:,3) .* mu_v(:,3);
        #  v(:,3) = a_v(:,2) .* (mu_v(:,3) + var_v(:,2));
        #  [m1 v1] = max(v(:,2));
        #  [m2 v2] = max(v(:,3));
        v[:, 1] = a_v[:, 2] * mu_v[:, 2]
        v[:, 2] = a_v[:, 1] * (mu_v[:, 2] + var_v[:, 1])
        v1 = np.argmax(v[:, 1])
        v2 = np.argmax(v[:, 2])
        a_vi = [a_v[v1, 1], a_v[v2, 1]]
        mu_vi = [mu_v[v1, 1], mu_v[v2, 1]]
        var_vi = [var_v[v1, 1], var_v[v2, 1]]
        #  quadratic equation for h1(p) - h2(p) = 0
        eq = [
            h_p2(e1, e2, a_vi[0], n_b, mu_vi[0], var_vi[0]) -
            h_p2(e1, e2, a_vi[1], n_b, mu_vi[1], var_vi[1]),
            h_p(e1, e2, a_vi[0], n_b, mu_vi[0], var_vi[0]) -
            h_p(e1, e2, a_vi[1], n_b, mu_vi[1], var_vi[1]),
            h_const(e1, e2, a_vi[0], n_b, mu_vi[0], var_vi[0]) -
            h_const(e1, e2, a_vi[1], n_b, mu_vi[1], var_vi[1])
        ]
        r = np.roots(eq)

        if len(r) == 0:
            sum1 = a_v[v1, 2] * mu_v[v1, 2] * n_b**2
            sum2 = a_v[v1, 2] * mu_v[v1, 2] * n_b
            sum3 = a_v[v1, 1] * (mu_v[v1, 2] + var_v[v1, 1]) * n_b**2
            sum4 = a_v[v1, 1] * (mu_v[v1, 2] + var_v[v1, 1]) * n_b
            sum5 = a_v[v1, 1] * (mu_v[v1, 2] + var_v[v1, 1]) * n_b
            val = e1 * e2 * (sum1 - sum2 - sum3 + sum4) / sum5
            val = math.sqrt(val) if val > 0 else 0 
            p = min([1, max([e1, e2, val])])
        else:
            p1 = r[0]
            p2 = r[1]
            sum1 = a_v[v1, 2] * mu_v[v1, 2] * n_b**2
            sum2 = a_v[v1, 2] * mu_v[v1, 2] * n_b
            sum3 = a_v[v1, 1] * (mu_v[v1, 2] + var_v[v1, 1]) * n_b**2
            sum4 = a_v[v1, 1] * (mu_v[v1, 2] + var_v[v1, 1]) * n_b
            sum5 = a_v[v1, 1] * (mu_v[v1, 2] + var_v[v1, 1]) * n_b
            val = e1 * e2 * (sum1 - sum2 - sum3 + sum4) / sum5
            val = math.sqrt(val) if val > 0 else 0 
            p3 = min([1, max([e1, e2, val])])

            sum1 = a_v[v2, 2] * mu_v[v2, 2] * n_b**2
            sum2 = a_v[v2, 2] * mu_v[v2, 2] * n_b
            sum3 = a_v[v2, 1] * (mu_v[v2, 2] + var_v[v2, 1]) * n_b**2
            sum4 = a_v[v2, 1] * (mu_v[v2, 2] + var_v[v2, 1]) * n_b
            sum5 = a_v[v2, 1] * (mu_v[v2, 2] + var_v[v2, 1]) * n_b
            val = e1 * e2 * (sum1 - sum2 - sum3 + sum4) / sum5
            val = math.sqrt(val) if val > 0 else 0 
            p4 = min([1, max([e1, e2, val])])

            p5 = max([e1, e2])

            pval = np.zeros((5, 2))
            pval[0, 0] = p1
            pval[1, 0] = p2
            pval[2, 0] = p3
            pval[3, 0] = p4
            pval[4, 0] = p5
            pval[0, 1] = max([
                h(p1, e1, e2, a_vi[0], n_b, mu_vi[0], var_vi[0]),
                h(p1, e1, e2, a_vi[1], n_b, mu_vi[1], var_vi[1])
            ])
            pval[1, 1] = max([
                h(p2, e1, e2, a_vi[0], n_b, mu_vi[0], var_vi[0]),
                h(p2, e1, e2, a_vi[1], n_b, mu_vi[1], var_vi[1])
            ])
            pval[2, 1] = max([
                h(p3, e1, e2, a_vi[0], n_b, mu_vi[0], var_vi[0]),
                h(p3, e1, e2, a_vi[1], n_b, mu_vi[1], var_vi[1])
            ])
            pval[3, 1] = max([
                h(p4, e1, e2, a_vi[0], n_b, mu_vi[0], var_vi[0]),
                h(p4, e1, e2, a_vi[1], n_b, mu_vi[1], var_vi[1])
            ])
            pval[4, 1] = max([
                h(p5, e1, e2, a_vi[0], n_b, mu_vi[0], var_vi[0]),
                h(p5, e1, e2, a_vi[1], n_b, mu_vi[1], var_vi[1])
            ])
            check_real = np.isreal(pval[:, 0])
            pval = np.delete(pval, np.argwhere(check_real is False), 0)
            pval = np.delete(pval, np.argwhere(pval[:, 0] < max([e1, e2])), 0)
            m = np.argmin(pval[:, 1])
            p = pval[m, 0]
    elif agg_type == 'avg':
        T2_group_by_count_sql = """SELECT {0}, COUNT(*) FROM {1}.{2} GROUP BY {0};
        """.format(T2_join_col, T2_schema, worst_table)
        cur.execute(T2_group_by_count_sql)
        while True:
            results = cur.fetchmany(fetch_size)
            if not results:
                break

            for row in results:
                col1 = row[0]
                cnt = row[1]
                b_v[col1 - 1, 1] = cnt
        b_v[:, 2] = b_v[:, 1]**2

        A_denom = sum(a_v[:, 1] * mu_v[:, 1] * b_v[:, 1])**2
        A1 = sum(a_v[:, 1] * (mu_v[:, 2] + var_v[:, 1]) * b_v[:, 1]) / A_denom
        A2 = sum(a_v[:, 2] * mu_v[:, 2] * b_v[:, 2]) / A_denom
        A3 = sum(a_v[:, 2] * mu_v[:, 2] * b_v[:, 1]) / A_denom
        A4 = sum(a_v[:, 1] * (mu_v[:, 2] + var_v[:, 1]) * b_v[:, 2])
        A = A1 + A2 - A3 - A4

        B_denom = sum(a_v[:, 1] * b_v[:, 1]) * sum(
            a_v[:, 1] * mu_v[:, 1] * b_v[:, 1])
        B1 = 1 / sum(a_v[:, 1] * b_v[:, 1])
        B2 = sum(a_v[:, 2] * mu_v[:, 1] * b_v[:, 2]) / B_denom
        B3 = sum(a_v[:, 2] * mu_v[:, 1] * b_v[:, 1]) / B_denom
        B4 = sum(a_v[:, 1] * mu_v[:, 1] * b_v[:, 2]) / B_denom
        B = B1 + B2 - B3 - B4

        C_denom = sum(a_v[:, 1] * b_v[:, 1])**2
        C1 = sum(a_v[:, 1] * b_v[:, 1]) / C_denom
        C2 = sum(a_v[:, 2] * b_v[:, 2]) / C_denom
        C3 = sum(a_v[:, 2] * b_v[:, 1]) / C_denom
        C4 = sum(a_v[:, 1] * b_v[:, 2]) / C_denom
        C = C1 + C2 - C3 - C4

        D = (1 / e1 * e2) * (A1 - 2 * B1 + C1)
        val1 = A - (2 * B) + C
        val2 = D

        if val1 <= 0 and val2 > 0:
            p = max(e1, e2)
        elif val1 > 0 and val2 <= 0:
            p = 1
        elif val1 > 0 and val2 > 0:
            val = math.sqrt((A - 2 * B + C) / D)
            p = min([1, max([e1, e2, val])])
        else:
            p_minus = max(e1, e2)
            p_plus = 1
            pval1 = (1 / p_minus) * val1 + p_minus * val2
            pval2 = (1 / p_plus) * val2 + p_plus * val2
            if pval1 < pval2:
                p = p_minus
            else:
                p = p_plus
        '''
        # estimate using sketch
        num_sketch = 1000
        v1 = 0  # sum(a_v[:,1] * mu_v[:,1] * b_v[:,1])
        v2 = 0  # sum(a_v[:,1] * (mu_v[:,2] + var_v[:,1]) * b_v[:,1])
        v3 = 0  # sum(a_v[:,2] * mu_v[:,2] * b_v[:,2])
        v4 = 0  # sum(a_v[:,2] * mu_v[:,2] * b_v[:,1])
        v5 = 0  # sum(a_v[:,1] * (mu_v[:,2] + var_v[:,1]) * b_v[:,2])
        v6 = 0  # sum(a_v[:,1] * b_v[:,1]))
        v7 = 0  # sum(a_v[:,2] * mu_v[:,1] * b_v[:,2])
        v8 = 0  # sum(a_v[:,2] * mu_v[:,1] * b_v[:,1])
        v9 = 0  # sum(a_v[:,1] * mu_v[:,1] * b_v[:,2])
        v10 = 0  # sum(a_v[:,2] * b_v[:,2]))
        v11 = 0  # sum(a_v[:,2] * b_v[:,1]))
        v12 = 0  # sum(a_v[:,1] * b_v[:,2]))
        for i in range(0, num_sketch):
            print("current_sketch = {}".format(i))
            x = np.random.randint(0, 2, size=num_keys)
            x[x == 0] = -1
            v1_1 = sum(a_v[:, 1] * mu_v[:, 1] * x[:])
            v1_2 = sum(b_v[:, 1] * x[:])
            v1 = v1 + (v1_1 * v1_2)
            v2_1 = sum(a_v[:, 1] * (mu_v[:, 2] + var_v[:, 1]) * x[:])
            v2_2 = v1_2
            v2 = v2 + (v2_1 * v1_2)
            v3_1 = sum(a_v[:, 2] * mu_v[:, 2] * x[:])
            v3_2 = sum(b_v[:, 2] * x[:])
            v3 = v3 + (v3_1 * v3_2)
            v4_1 = v3_1
            v4_2 = v1_2
            v4 = v4 + (v4_1 * v4_2)
            v5_1 = v2_1
            v5_2 = v3_2
            v5 = v5 + (v5_1 * v5_2)
            v6_1 = sum(a_v[:, 1] * x[:])
            v6_2 = v1_2
            v6 = v6 + (v6_1 * v6_2)
            v7_1 = sum(a_v[:, 2] * mu_v[:, 1] * x[:])
            v7_2 = v3_2
            v7 = v7 + (v7_1 * v7_2)
            v8_1 = v7_1
            v8_2 = v1_2
            v8 = v8 + (v8_1 * v8_2)
            v9_1 = v1_1
            v9_2 = v3_2
            v9 = v9 + (v9_1 * v9_2)
            v10_1 = sum(a_v[:, 2] * x[:])
            v10_2 = v3_2
            v10 = v10 + (v10_1 * v10_2)
            v11_1 = v10_1
            v11_2 = v1_2
            v11 = v11 + (v11_1 * v11_2)
            v12_1 = v6_1
            v12_2 = v3_2
            v12 = v12 + (v12_1 * v12_2)

        v1 = v1 / num_sketch
        v2 = v2 / num_sketch
        v3 = v3 / num_sketch
        v4 = v4 / num_sketch
        v5 = v5 / num_sketch
        v6 = v6 / num_sketch
        v7 = v7 / num_sketch
        v8 = v8 / num_sketch
        v9 = v9 / num_sketch
        v10 = v10 / num_sketch
        v11 = v11 / num_sketch
        v12 = v12 / num_sketch

        A_denom = v1**2
        A1 = v2 / A_denom
        A2 = v3 / A_denom
        A3 = v4 / A_denom
        A4 = v5 / A_denom
        A = A1 + A2 - A3 - A4

        B_denom = v6 * v1
        B1 = 1 / v6
        B2 = v7 / B_denom
        B3 = v8 / B_denom
        B4 = v9 / B_denom
        B = B1 + B2 - B3 - B4

        C_denom = v6**2
        C1 = v6 / C_denom
        C2 = v10 / C_denom
        C3 = v11 / C_denom
        C4 = v12 / C_denom
        C = C1 + C2 - C3 - C4

        D = (1 / e1 * e2) * (A1 - (2 * B1) + C1)

        val1 = A - (2 * B) + C
        val2 = D

        if val1 <= 0 and val2 > 0:
            p = max(e1, e2)
        elif val1 > 0 and val2 <= 0:
            p = 1
        elif val1 > 0 and val2 > 0:
            val = (A - (2 * B) + C) / D
            if val > 0:
                val = math.sqrt(val)
            p = min([1, max([e1, e2, val])])
        else:
            p_minus = max(e1, e2)
            p_plus = 1
            pval1 = (1 / p_minus) * val1 + p_minus * val2
            pval2 = (1 / p_plus) * val1 + p_plus * val2
            if pval1 < pval2:
                p = p_minus
            else:
                p = p_plus
        '''
    else:
        print('Unsupported type: {}'.format(agg_type))
        raise ValueError

    q1 = e1 / p
    q2 = e2 / p
    t3 = datetime.datetime.now()
    #  hash_num = np.random.randint(1000 * 1000 * 1000)
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS {}".format(sample_schema))

    tables = get_existing_tables(conn, sample_schema)
    m = 1540483477
    modval = 2**32
    for i in range(1, num_sample + 1):
        hash_num = np.random.randint(1000 * 1000)
        hash_num2 = np.random.randint(1000 * 1000)
        S1_name = "s__{}__{}__{}__{}__{}__{}__{}".format(
            T1_table, T2_table, T1_join_col, T1_agg_col, agg_type, i, 1)
        S2_name = "s__{}__{}__{}__{}__{}__{}".format(T1_table, T2_table,
                                                     T2_join_col, agg_type, i,
                                                     2)

        if overwrite:
            cur.execute("DROP TABLE IF EXISTS {}.{}".format(
                sample_schema, S1_name))
            cur.execute("DROP TABLE IF EXISTS {}.{}".format(
                sample_schema, S2_name))
            conn.commit()
        else:
            if S1_name in tables and S2_name in tables:
                continue

        create_S1_sql = """CREATE TABLE IF NOT EXISTS {0}.{1} STORED AS PARQUET AS SELECT * FROM (
        SELECT *, pmod(bitxor(bitxor({7} * {9}, rotateright({7} * {9}, 24)) * {9}, {6} * {9}), {10}) as pval
        FROM {2}.{3}
        ) tmp
        WHERE pval <= {4} * {10} and rand(unix_timestamp() + {6} + 1) <= {5}
        """.format(sample_schema, S1_name, T1_schema, T1_table, p, q1,
                   hash_num + i, T1_join_col, hash_num2, m, modval)
        create_S2_sql = """CREATE TABLE IF NOT EXISTS {0}.{1} STORED AS PARQUET AS SELECT * FROM (
        SELECT *, pmod(bitxor(bitxor({7} * {9}, rotateright({7} * {9}, 24)) * {9}, {6} * {9}), {10}) as pval
        FROM {2}.{3}
        ) tmp
        WHERE pval <= {4} * {10} and rand(unix_timestamp() + {6} + 1) <= {5}
        """.format(sample_schema, S2_name, T2_schema, T2_table, p, q2,
                   hash_num + i, T2_join_col, hash_num2, m, modval)

        #  create_S1_sql = """CREATE TABLE IF NOT EXISTS {0}.{1} STORED AS PARQUET AS SELECT * FROM (
        #  SELECT *, rand(unix_timestamp() + {6} + 1) as qval, pmod(fnv_hash({7} + {6}), 1000*1000*1000*1000) as pval
        #  FROM {2}.{3} {8}
        #  ) tmp
        #  WHERE pval <= {4} * 1000*1000*1000*1000 and qval <= {5}
        #  """.format(sample_schema, S1_name, T1_schema, T1_table, p, q1,
        #  hash_num + i, T1_join_col, T1_where)
        #
        #  create_S2_sql = """CREATE TABLE IF NOT EXISTS {0}.{1} STORED AS PARQUET AS SELECT * FROM (
        #  SELECT *, rand(unix_timestamp() + {6} + 2) as qval, pmod(fnv_hash({7} + {6}), 1000*1000*1000*1000) as pval
        #  FROM {2}.{3}
        #  ) tmp
        #  WHERE pval <= {4} * 1000*1000*1000*1000 and qval <= {5}
        #  """.format(sample_schema, S2_name, T2_schema, T2_table, p, q2,
        #  hash_num + i, T2_join_col)

        cur.execute(create_S1_sql)
        cur.execute(create_S2_sql)
        conn.commit()

        #  cur.execute("COMPUTE STATS {}.{}".format(sample_schema, S1_name))
        #  cur.execute("COMPUTE STATS {}.{}".format(sample_schema, S2_name))

    # add metadata
    create_metatable_sql = """CREATE TABLE IF NOT EXISTS {}.meta (t1_name STRING, t2_name STRING, agg STRING,
    t1_join_col STRING, t2_join_col STRING, t1_agg_col STRING, t1_where_col STRING,
    p DOUBLE, q DOUBLE, ts TIMESTAMP ) STORED AS PARQUET
    """.format(sample_schema)
    cur.execute(create_metatable_sql)

    ts = int(time.time())
    insert_meta = """INSERT INTO TABLE {}.meta VALUES ('{}','{}','{}','{}','{}','{}',{},{},{},now())""".format(
        sample_schema, T1_table, T2_table, agg_type, T1_join_col, T2_join_col,
        T1_agg_col, 'NULL', p, q1)
    cur.execute(insert_meta)
    cur.close()
    t4 = datetime.datetime.now()
    if print_time:
        print("start datetime: {}".format(str(t1)))
        print("collecting frequency info: {} s".format(
            (t2 - t1).total_seconds()))
        print("calculating p and q: {} s".format((t3 - t2).total_seconds()))
        print("creating samples: {} s".format((t4 - t3).total_seconds()))
        print("total: {} s".format((t4 - t1).total_seconds()))
        print("end datetime: {}".format(str(t4)))


def create_cent_sample_pair_from_impala(host,
                                        port,
                                        T1_schema,
                                        T1_table,
                                        T1_join_col,
                                        T1_agg_col,
                                        T2_schema,
                                        T2_table,
                                        T2_join_col,
                                        T1_where,
                                        sample_schema,
                                        agg_type,
                                        num_sample,
                                        overwrite=False,
                                        print_time=False):
    t1 = datetime.datetime.now()
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
    T1_group_by_count_sql = """SELECT {0}, COUNT(*), avg({1}), variance({1}) FROM {2}.{3} {4} GROUP BY {0};
    """.format(T1_join_col, T1_agg_col, T1_schema, T1_table, T1_where)
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
    mu_v = np.nan_to_num(mu_v)
    var_v = np.nan_to_num(var_v)

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
    t2 = datetime.datetime.now()

    if agg_type == 'count':
        sum1 = sum(a_v[:, 1] * b_v[:, 1] - a_v[:, 1] * b_v[:, 0] -
                   a_v[:, 0] * b_v[:, 1] + a_v[:, 0] * b_v[:, 0])
        sum2 = sum(a_v[:, 0] * b_v[:, 0])
        val = e1 * e2 * sum1 / sum2
        if val > 0:
            val = math.sqrt(val)
        p = min([1, max([e1, e2, val])])
    elif agg_type == 'sum':
        sum1 = sum(a_v[:, 1] * mu_v[:, 1] * b_v[:, 1])
        sum2 = sum(a_v[:, 1] * mu_v[:, 1] * b_v[:, 0])
        sum3 = sum(a_v[:, 0] * (mu_v[:, 1] + var_v[:, 0]) * b_v[:, 1])
        sum4 = sum(a_v[:, 0] * (mu_v[:, 1] + var_v[:, 0]) * b_v[:, 0])
        sum5 = sum(a_v[:, 0] * (mu_v[:, 1] + var_v[:, 0]) * b_v[:, 0])
        val = e1 * e2 * (sum1 - sum2 - sum3 + sum4) / sum5
        val = math.sqrt(val) if val > 0 else val
        p = min([1, max([e1, e2, val])])
    elif agg_type == 'avg':
        A_denom = sum(a_v[:, 0] * mu_v[:, 0] * b_v[:, 0])**2
        A1 = sum(a_v[:, 0] * (mu_v[:, 1] + var_v[:, 0]) * b_v[:, 0]) / A_denom
        A2 = sum(a_v[:, 1] * mu_v[:, 1] * b_v[:, 1]) / A_denom
        A3 = sum(a_v[:, 1] * mu_v[:, 1] * b_v[:, 0]) / A_denom
        A4 = sum(a_v[:, 0] * (mu_v[:, 1] + var_v[:, 0]) * b_v[:, 1])
        A = A1 + A2 - A3 - A4

        B_denom = sum(a_v[:, 0] * b_v[:, 0]) * sum(
            a_v[:, 0] * mu_v[:, 0] * b_v[:, 0])
        B1 = 1 / sum(a_v[:, 0] * b_v[:, 0])
        B2 = sum(a_v[:, 1] * mu_v[:, 0] * b_v[:, 1]) / B_denom
        B3 = sum(a_v[:, 1] * mu_v[:, 0] * b_v[:, 0]) / B_denom
        B4 = sum(a_v[:, 0] * mu_v[:, 0] * b_v[:, 1]) / B_denom
        B = B1 + B2 - B3 - B4

        C_denom = sum(a_v[:, 0] * b_v[:, 0])**2
        C1 = sum(a_v[:, 0] * b_v[:, 0]) / C_denom
        C2 = sum(a_v[:, 1] * b_v[:, 1]) / C_denom
        C3 = sum(a_v[:, 1] * b_v[:, 0]) / C_denom
        C4 = sum(a_v[:, 0] * b_v[:, 1]) / C_denom
        C = C1 + C2 - C3 - C4

        D = (1 / e1 * e2) * (A1 - 2 * B1 + C1)
        val1 = A - (2 * B) + C
        val2 = D

        if val1 <= 0 and val2 > 0:
            p = max(e1, e2)
        elif val1 > 0 and val2 <= 0:
            p = 1
        elif val1 > 0 and val2 > 0:
            val = math.sqrt((A - 2 * B + C) / D)
            p = min([1, max([e1, e2, val])])
        else:
            p_minus = max(e1, e2)
            p_plus = 1
            pval1 = (1 / p_minus) * val1 + p_minus * val2
            pval2 = (1 / p_plus) * val2 + p_plus * val2
            if pval1 < pval2:
                p = p_minus
            else:
                p = p_plus
    else:
        print("Unsupported operation")
        return

    q1 = e1 / p
    q2 = e2 / p
    t3 = datetime.datetime.now()
    est = estimate_variance(a_v, b_v, mu_v, var_v, p)
    print((T1_table, T2_table, est))
    return
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS {}".format(sample_schema))

    tables = get_existing_tables(conn, sample_schema)
    m = 1540483477
    modval = 2**32
    for i in range(1, num_sample + 1):
        hash_num = np.random.randint(1000 * 1000)
        hash_num2 = np.random.randint(1000 * 1000)
        S1_name = "s__{}__{}__{}__{}__{}__{}__{}".format(
            T1_table, T2_table, T1_join_col, T1_agg_col, agg_type, i, 1)
        S2_name = "s__{}__{}__{}__{}__{}__{}".format(T1_table, T2_table,
                                                     T2_join_col, agg_type, i,
                                                     2)

        if overwrite:
            cur.execute("DROP TABLE IF EXISTS {}.{}".format(
                sample_schema, S1_name))
            cur.execute("DROP TABLE IF EXISTS {}.{}".format(
                sample_schema, S2_name))
            conn.commit()
        else:
            if S1_name in tables and S2_name in tables:
                continue

        create_S1_sql = """CREATE TABLE IF NOT EXISTS {0}.{1} STORED AS PARQUET AS SELECT * FROM (
        SELECT *, pmod(bitxor(bitxor({7} * {9}, rotateright({7} * {9}, 24)) * {9}, {6} * {9}), {10}) as pval
        FROM {2}.{3}
        ) tmp
        WHERE pval <= {4} * {10} and rand(unix_timestamp() + {6} + 1) <= {5}
        """.format(sample_schema, S1_name, T1_schema, T1_table, p, q1,
                   hash_num + i, T1_join_col, hash_num2, m, modval)
        create_S2_sql = """CREATE TABLE IF NOT EXISTS {0}.{1} STORED AS PARQUET AS SELECT * FROM (
        SELECT *, pmod(bitxor(bitxor({7} * {9}, rotateright({7} * {9}, 24)) * {9}, {6} * {9}), {10}) as pval
        FROM {2}.{3}
        ) tmp
        WHERE pval <= {4} * {10} and rand(unix_timestamp() + {6} + 1) <= {5}
        """.format(sample_schema, S2_name, T2_schema, T2_table, p, q2,
                   hash_num + i, T2_join_col, hash_num2, m, modval)

        #  create_S1_sql = """CREATE TABLE IF NOT EXISTS {0}.{1} STORED AS PARQUET AS SELECT * FROM (
        #  SELECT *, pmod(fnv_hash({7} + {6}), 1000*1000)+1 as pval
        #  FROM {2}.{3}
        #  ) tmp
        #  WHERE pval <= {4} * 1000*1000 and rand(unix_timestamp() + {6} + 1) <= {5}
        #  """.format(sample_schema, S1_name, T1_schema, T1_table, p, q1,
                   #  hash_num + i, T1_join_col)
#
        #  create_S2_sql = """CREATE TABLE IF NOT EXISTS {0}.{1} STORED AS PARQUET AS SELECT * FROM (
        #  SELECT *, pmod(fnv_hash({7} + {6}), 1000*1000)+1 as pval
        #  FROM {2}.{3}
        #  ) tmp
        #  WHERE pval <= {4} * 1000*1000 and rand(unix_timestamp() + {6} + 2) <= {5}
        #  """.format(sample_schema, S2_name, T2_schema, T2_table, p, q2,
                   #  hash_num + i, T2_join_col)
        cur.execute(create_S1_sql)
        cur.execute(create_S2_sql)
        conn.commit()

        #  cur.execute("COMPUTE STATS {}.{}".format(sample_schema, S1_name))
        #  cur.execute("COMPUTE STATS {}.{}".format(sample_schema, S2_name))

    # add metadata
    create_metatable_sql = """CREATE TABLE IF NOT EXISTS {}.meta (t1_name STRING, t2_name STRING, agg STRING,
    t1_join_col STRING, t2_join_col STRING, t1_agg_col STRING, t1_where_col STRING,
    p DOUBLE, q DOUBLE, ts TIMESTAMP ) STORED AS PARQUET
    """.format(sample_schema)
    cur.execute(create_metatable_sql)

    ts = int(time.time())
    insert_meta = """INSERT INTO TABLE {}.meta VALUES ('{}','{}','{}','{}','{}','{}',{},{},{},now())""".format(
        sample_schema, T1_table, T2_table, agg_type, T1_join_col, T2_join_col,
        T1_agg_col, 'NULL', p, q1)
    cur.execute(insert_meta)
    cur.close()
    t4 = datetime.datetime.now()
    if print_time:
        print("start datetime: {}".format(str(t1)))
        print("collecting frequency info: {} s".format(
            (t2 - t1).total_seconds()))
        print("calculating p and q: {} s".format((t3 - t2).total_seconds()))
        print("creating samples: {} s".format((t4 - t3).total_seconds()))
        print("total: {} s".format((t4 - t1).total_seconds()))
        print("end datetime: {}".format(str(t4)))


def create_cent_sample_pair_with_where_from_impala(host,
                                                   port,
                                                   T1_schema,
                                                   T1_table,
                                                   T1_join_col,
                                                   T1_agg_col,
                                                   T2_schema,
                                                   T2_table,
                                                   T2_join_col,
                                                   T1_where_col,
                                                   sample_schema,
                                                   agg_type,
                                                   cond_type,
                                                   where_dist_type,
                                                   num_sample,
                                                   overwrite=False):
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

    b_v = np.zeros((num_keys, 2))
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
    increment = 1

    keys = np.arange(1, num_keys + 1)
    if cond_type == 'eq':
        cond_op = '='
        if T1_table != 'instacart':
            increment = 5
    elif cond_type == 'geq':
        cond_op = '>='
        if T1_table != 'instacart':
            increment = 10
    else:
        print("Unsupported Cond Op: {}".format(cond_type))
        return

    cur = conn.cursor()
    cur.execute("SELECT MIN({0}), MAX({0}) FROM {1}.{2}".format(
        T1_where_col, T1_schema, T1_table))
    row = cur.fetchone()
    start_val = row[0]
    end_val = row[1]

    val1 = 0
    val2 = 0
    ratio = 1

    if where_dist_type == 'identical':
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM {}.{}".format(T1_schema, T1_table))
        row = cur.fetchone()
        T1_total = row[0]
        cur.close()

    cur = conn.cursor()
    for cond_val in range(start_val, end_val + 1, increment):
        a_v = np.zeros((num_keys, 2))
        mu_v = np.zeros((num_keys, 2))
        var_v = np.zeros((num_keys, 2))

        if where_dist_type == 'identical':
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM {}.{} WHERE {} {} {}".format(
                T1_schema, T1_table, T1_where_col, cond_op, cond_val))
            row = cur.fetchone()
            c = row[0]
            ratio = c / T1_total

        T1_group_by_count_sql = """SELECT {0}, COUNT(*), avg({1}), variance({1})
        FROM {2}.{3} WHERE {4} {5} {6} GROUP BY {0};
        """.format(T1_join_col, T1_agg_col, T1_schema, T1_table, T1_where_col,
                   cond_op, cond_val)
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
        mu_v = np.nan_to_num(mu_v)
        var_v = np.nan_to_num(var_v)

        p = 0

        if agg_type == 'count':
            sum1 = sum(a_v[:, 1] * b_v[:, 1] - a_v[:, 1] * b_v[:, 0] -
                       a_v[:, 0] * b_v[:, 1] + a_v[:, 0] * b_v[:, 0])
            sum2 = sum(a_v[:, 0] * b_v[:, 0])
            val1 = val1 + (ratio * e1 * e2 * sum1)
            # val2 = val2 + (ratio * e1 * e2 * sum2)
            val2 = val2 + (ratio * sum2)
        elif agg_type == 'sum':
            sum1 = sum(a_v[:, 1] * mu_v[:, 1] * b_v[:, 1])
            sum2 = sum(a_v[:, 1] * mu_v[:, 1] * b_v[:, 0])
            sum3 = sum(a_v[:, 0] * (mu_v[:, 1] + var_v[:, 0]) * b_v[:, 1])
            sum4 = sum(a_v[:, 0] * (mu_v[:, 1] + var_v[:, 0]) * b_v[:, 0])
            sum5 = sum(a_v[:, 0] * (mu_v[:, 1] + var_v[:, 0]) * b_v[:, 0])
            val1 = val1 + ratio * e1 * e2 * (sum1 - sum2 - sum3 + sum4)
            # val2 = val2 + ratio * e1 * e2 * sum5
            val2 = val2 + ratio * sum5
        elif agg_type == 'avg':
            A_denom = sum(a_v[:, 0] * mu_v[:, 0] * b_v[:, 0])**2
            A1 = sum(a_v[:, 0] *
                     (mu_v[:, 1] + var_v[:, 0]) * b_v[:, 0]) / A_denom
            A2 = sum(a_v[:, 1] * mu_v[:, 1] * b_v[:, 1]) / A_denom
            A3 = sum(a_v[:, 1] * mu_v[:, 1] * b_v[:, 0]) / A_denom
            A4 = sum(a_v[:, 0] * (mu_v[:, 1] + var_v[:, 0]) * b_v[:, 1])
            A = A1 + A2 - A3 - A4

            B_denom = sum(a_v[:, 0] * b_v[:, 0]) * sum(
                a_v[:, 0] * mu_v[:, 0] * b_v[:, 0])
            B1 = 1 / sum(a_v[:, 0] * b_v[:, 0])
            B2 = sum(a_v[:, 1] * mu_v[:, 0] * b_v[:, 1]) / B_denom
            B3 = sum(a_v[:, 1] * mu_v[:, 0] * b_v[:, 0]) / B_denom
            B4 = sum(a_v[:, 0] * mu_v[:, 0] * b_v[:, 1]) / B_denom
            B = B1 + B2 - B3 - B4

            C_denom = sum(a_v[:, 0] * b_v[:, 0])**2
            C1 = sum(a_v[:, 0] * b_v[:, 0]) / C_denom
            C2 = sum(a_v[:, 1] * b_v[:, 1]) / C_denom
            C3 = sum(a_v[:, 1] * b_v[:, 0]) / C_denom
            C4 = sum(a_v[:, 0] * b_v[:, 1]) / C_denom
            C = C1 + C2 - C3 - C4

            D = (1 / e1 * e2) * (A1 - 2 * B1 + C1)
            v1 = A - (2 * B) + C
            v2 = D

            val1 = val1 + (ratio * v1)
            val2 = val2 + (ratio * v2)
        else:
            print("Unsupported operation")
            return

    if val1 <= 0 and val2 > 0:
        p = max(e1, e2)
    elif val1 > 0 and val2 <= 0:
        p = 1
    elif val1 > 0 and val2 > 0:
        val = math.sqrt(val1 / val2)
        p = min([1, max([e1, e2, val])])
    else:
        p_minus = max(e1, e2)
        p_plus = 1
        pval1 = (1 / p_minus) * val1 + p_minus * val2
        pval2 = (1 / p_plus) * val2 + p_plus * val2
        if pval1 < pval2:
            p = p_minus
        else:
            p = p_plus

    q1 = e1 / p
    q2 = e2 / p
    print ((val1, val2, p, q1))
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS {}".format(sample_schema))
    tables = get_existing_tables(conn, sample_schema)

    m = 1540483477
    modval = 2**32
    for i in range(1, num_sample + 1):
        hash_num = np.random.randint(1000 * 1000)
        hash_num2 = np.random.randint(1000 * 1000)
        S1_name = "s__{}__{}__{}__{}__{}__{}__{}__{}__{}".format(
            T1_table, T2_table, T1_join_col, T1_agg_col, agg_type, cond_type,
            where_dist_type, i, 1)
        S2_name = "s__{}__{}__{}__{}__{}__{}__{}__{}".format(
            T1_table, T2_table, T2_join_col, agg_type, cond_type,
            where_dist_type, i, 2)

        if overwrite:
            cur.execute("DROP TABLE IF EXISTS {}.{}".format(
                sample_schema, S1_name))
            cur.execute("DROP TABLE IF EXISTS {}.{}".format(
                sample_schema, S2_name))
            conn.commit()
        else:
            if S1_name in tables and S2_name in tables:
                continue

        create_S1_sql = """CREATE TABLE IF NOT EXISTS {0}.{1} STORED AS PARQUET AS SELECT * FROM (
        SELECT *, pmod(bitxor(bitxor({7} * {9}, rotateright({7} * {9}, 24)) * {9}, {6} * {9}), {10}) as pval
        FROM {2}.{3}
        ) tmp
        WHERE pval <= {4} * {10} and rand(unix_timestamp() + {6} + 1) <= {5}
        """.format(sample_schema, S1_name, T1_schema, T1_table, p, q1,
                   hash_num + i, T1_join_col, hash_num2, m, modval)
        create_S2_sql = """CREATE TABLE IF NOT EXISTS {0}.{1} STORED AS PARQUET AS SELECT * FROM (
        SELECT *, pmod(bitxor(bitxor({7} * {9}, rotateright({7} * {9}, 24)) * {9}, {6} * {9}), {10}) as pval
        FROM {2}.{3}
        ) tmp
        WHERE pval <= {4} * {10} and rand(unix_timestamp() + {6} + 1) <= {5}
        """.format(sample_schema, S2_name, T2_schema, T2_table, p, q2,
                   hash_num + i, T2_join_col, hash_num2, m, modval)

        #  create_S1_sql = """CREATE TABLE IF NOT EXISTS {0}.{1} STORED AS PARQUET AS SELECT * FROM (
        #  SELECT *, rand(unix_timestamp() + {6} + 1) as qval, pmod(fnv_hash({7} + {6}), 1000*1000)+1 as pval
        #  FROM {2}.{3}
        #  ) tmp
        #  WHERE pval <= {4} * 1000*1000 and qval <= {5}
        #  """.format(sample_schema, S1_name, T1_schema, T1_table, p, q1,
                   #  hash_num + i, T1_join_col)
#
        #  create_S2_sql = """CREATE TABLE IF NOT EXISTS {0}.{1} STORED AS PARQUET AS SELECT * FROM (
        #  SELECT *, rand(unix_timestamp() + {6} + 2) as qval, pmod(fnv_hash({7} + {6}), 1000*1000)+1 as pval
        #  FROM {2}.{3}
        #  ) tmp
        #  WHERE pval <= {4} * 1000*1000 and qval <= {5}
        #  """.format(sample_schema, S2_name, T2_schema, T2_table, p, q2,
                   #  hash_num + i, T2_join_col)
#
        cur.execute(create_S1_sql)
        cur.execute(create_S2_sql)
        conn.commit()

        #  cur.execute("COMPUTE STATS {}.{}".format(sample_schema, S1_name))
        #  cur.execute("COMPUTE STATS {}.{}".format(sample_schema, S2_name))

    # add metadata
    create_metatable_sql = """CREATE TABLE IF NOT EXISTS {}.meta (t1_name STRING, t2_name STRING, agg STRING,
    t1_join_col STRING, t2_join_col STRING, t1_agg_col STRING, t1_where_col STRING,
    cond_type STRING, where_dist STRING,
    p DOUBLE, q DOUBLE, ts TIMESTAMP ) STORED AS PARQUET
    """.format(sample_schema)
    cur.execute(create_metatable_sql)

    ts = int(time.time())
    insert_meta = """INSERT INTO TABLE {}.meta VALUES
    ('{}','{}','{}','{}','{}','{}','{}','{}','{}',{},{},now())""".format(
        sample_schema, T1_table, T2_table, agg_type, T1_join_col, T2_join_col,
        T1_agg_col, T1_where_col, cond_type, where_dist_type, p, q1)
    cur.execute(insert_meta)
    conn.commit()
    cur.close()


def create_cent_sample_pair_for_all_from_impala(host,
                                                port,
                                                T1_schema,
                                                T1_table,
                                                T1_join_col,
                                                T1_agg_col_sum,
                                                T1_agg_col_avg,
                                                T2_schema,
                                                T2_table,
                                                T2_join_col,
                                                sample_schema,
                                                num_sample,
                                                overwrite=False):
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
    mu_sum_v = np.zeros((num_keys, 2))
    var_sum_v = np.zeros((num_keys, 2))
    mu_avg_v = np.zeros((num_keys, 2))
    var_avg_v = np.zeros((num_keys, 2))

    keys = np.arange(1, num_keys + 1)

    cur = conn.cursor()
    T1_group_by_count_sql = """SELECT {0}, COUNT(*), avg({1}), variance({1}), avg({2}), variance({2}) 
    FROM {3}.{4} GROUP BY {0};
    """.format(T1_join_col, T1_agg_col_sum, T1_agg_col_avg, T1_schema, T1_table)
    cur.execute(T1_group_by_count_sql)

    while True:
        results = cur.fetchmany(fetch_size)
        if not results:
            break

        for row in results:
            col1 = row[0]
            cnt = row[1]
            mean_sum = row[2]
            var_sum = row[3]
            mean_avg = row[4]
            var_avg = row[5]
            a_v[col1 - 1, 0] = cnt
            mu_sum_v[col1 - 1, 0] = mean_sum
            if var_sum is None:
                var_sum = 0
            var_sum_v[col1 - 1, 0] = var_sum
            mu_avg_v[col1 - 1, 0] = mean_avg
            if var_avg is None:
                var_avg = 0
            var_avg_v[col1 - 1, 0] = var_avg
    a_v[:, 1] = a_v[:, 0]**2
    mu_sum_v[:, 1] = mu_sum_v[:, 0]**2
    mu_avg_v[:, 1] = mu_avg_v[:, 0]**2
    mu_sum_v = np.nan_to_num(mu_sum_v)
    mu_avg_v = np.nan_to_num(mu_avg_v)
    var_sum_v = np.nan_to_num(var_sum_v)
    var_avg_v = np.nan_to_num(var_avg_v)

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

    # for COUNT
    sum1 = sum(a_v[:, 1] * b_v[:, 1] - a_v[:, 1] * b_v[:, 0] -
               a_v[:, 0] * b_v[:, 1] + a_v[:, 0] * b_v[:, 0])
    sum2 = sum(a_v[:, 0] * b_v[:, 0])
    count_val1 = e1 * e2 * sum1
    # count_val2 = e1 * e2 * sum2
    count_val2 =  sum2

    # for SUM
    sum1 = sum(a_v[:, 1] * mu_sum_v[:, 1] * b_v[:, 1])
    sum2 = sum(a_v[:, 1] * mu_sum_v[:, 1] * b_v[:, 0])
    sum3 = sum(a_v[:, 0] * (mu_sum_v[:, 1] + var_sum_v[:, 0]) * b_v[:, 1])
    sum4 = sum(a_v[:, 0] * (mu_sum_v[:, 1] + var_sum_v[:, 0]) * b_v[:, 0])
    sum5 = sum(a_v[:, 0] * (mu_sum_v[:, 1] + var_sum_v[:, 0]) * b_v[:, 0])
    sum_val1 = e1 * e2 * (sum1 - sum2 - sum3 + sum4)
    # sum_val2 = e1 * e2 * sum5
    sum_val2 = sum5

    # for AVG
    A_denom = sum(a_v[:, 0] * mu_avg_v[:, 0] * b_v[:, 0])**2
    A1 = sum(a_v[:, 0] * (mu_avg_v[:, 1] + var_avg_v[:, 0]) * b_v[:, 0]) / A_denom
    A2 = sum(a_v[:, 1] * mu_avg_v[:, 1] * b_v[:, 1]) / A_denom
    A3 = sum(a_v[:, 1] * mu_avg_v[:, 1] * b_v[:, 0]) / A_denom
    A4 = sum(a_v[:, 0] * (mu_avg_v[:, 1] + var_avg_v[:, 0]) * b_v[:, 1])
    A = A1 + A2 - A3 - A4

    B_denom = sum(a_v[:, 0] * b_v[:, 0]) * sum(
        a_v[:, 0] * mu_avg_v[:, 0] * b_v[:, 0])
    B1 = 1 / sum(a_v[:, 0] * b_v[:, 0])
    B2 = sum(a_v[:, 1] * mu_avg_v[:, 0] * b_v[:, 1]) / B_denom
    B3 = sum(a_v[:, 1] * mu_avg_v[:, 0] * b_v[:, 0]) / B_denom
    B4 = sum(a_v[:, 0] * mu_avg_v[:, 0] * b_v[:, 1]) / B_denom
    B = B1 + B2 - B3 - B4

    C_denom = sum(a_v[:, 0] * b_v[:, 0])**2
    C1 = sum(a_v[:, 0] * b_v[:, 0]) / C_denom
    C2 = sum(a_v[:, 1] * b_v[:, 1]) / C_denom
    C3 = sum(a_v[:, 1] * b_v[:, 0]) / C_denom
    C4 = sum(a_v[:, 0] * b_v[:, 1]) / C_denom
    C = C1 + C2 - C3 - C4

    D = (1 / e1 * e2) * (A1 - 2 * B1 + C1)
    avg_val1 = A - (2 * B) + C
    avg_val2 = D

    # estimate estimators using sketch
    num_sketch = 100
    sum_estimate1 = 0  # sum(a_v[:,1] * mu_v[:,1] * b_v[:,1]) for SUM
    sum_estimate2 = 0  # sum(a_v[:,1] * mu_v[:,1] * b_v[:,1]) for AVG
    count_estimate = 0  # sum(a_v[:,1] * b_v[:,1]))
    for i in range(1, num_sketch + 1):
        x = np.random.randint(0, 2, size=num_keys)
        x[x == 0] = -1
        v1_1_sum = sum(a_v[:, 0] * mu_sum_v[:, 0] * x[:])
        v1_1_avg = sum(a_v[:, 0] * mu_avg_v[:, 0] * x[:])
        v1_2 = sum(b_v[:, 0] * x[:])
        sum_estimate1 = sum_estimate1 + (v1_1_sum * v1_2)
        sum_estimate2 = sum_estimate2 + (v1_1_avg * v1_2)
        v2_1 = sum(a_v[:, 0] * x[:])
        v2_2 = v1_2
        count_estimate = count_estimate + (v2_1 * v2_2)
    sum_estimate = sum_estimate1 / num_sketch
    count_estimate = count_estimate / num_sketch
    sum_estimate2 = sum_estimate2 / num_sketch
    avg_estimate = sum_estimate2 / count_estimate

    print((count_val1, sum_val1, avg_val1))
    print((count_val2, sum_val2, avg_val2))
    print((count_estimate, sum_estimate, avg_estimate))

    count_val1 = count_val1 / (count_estimate**2)
    count_val2 = count_val2 / (count_estimate**2)
    sum_val1 = sum_val1 / (sum_estimate**2)
    sum_val2 = sum_val2 / (sum_estimate**2)
    avg_val1 = avg_val1 / (avg_estimate**2)
    avg_val2 = avg_val2 / (avg_estimate**2)

    print((count_val1, sum_val1, avg_val1))
    print((count_val2, sum_val2, avg_val2))

    val1 = count_val1 + sum_val1 + avg_val1
    val2 = count_val2 + sum_val2 + avg_val2


    if val1 <= 0 and val2 > 0:
        p = max(e1, e2)
    elif val1 > 0 and val2 <= 0:
        p = 1
    elif val1 > 0 and val2 > 0:
        val = math.sqrt(val1 / val2)
        p = min([1, max([e1, e2, val])])
    else:
        p_minus = max(e1, e2)
        p_plus = 1
        pval1 = (1 / p_minus) * val1 + p_minus * val2
        pval2 = (1 / p_plus) * val2 + p_plus * val2
        if pval1 < pval2:
            p = p_minus
        else:
            p = p_plus

    q1 = e1 / p
    q2 = e2 / p
    print((T1_table, T2_table, val1, val2, p, q1))
    #  hash_num = np.random.randint(1000 * 1000 * 1000)
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS {}".format(sample_schema))
    tables = get_existing_tables(conn, sample_schema)

    m = 1540483477
    modval = 2**32
    for i in range(1, num_sample + 1):
        hash_num = np.random.randint(1000 * 1000)
        hash_num2 = np.random.randint(1000 * 1000)
        S1_name = "s__{}__{}__{}__{}__{}__{}".format(
            T1_table, T2_table, T1_join_col, 'all', i, 1)
        S2_name = "s__{}__{}__{}__{}__{}__{}".format(T1_table, T2_table,
                                                     T2_join_col, 'all', i, 2)

        if overwrite:
            cur.execute("DROP TABLE IF EXISTS {}.{}".format(
                sample_schema, S1_name))
            cur.execute("DROP TABLE IF EXISTS {}.{}".format(
                sample_schema, S2_name))
            conn.commit()
        else:
            if S1_name in tables and S2_name in tables:
                continue

        create_S1_sql = """CREATE TABLE IF NOT EXISTS {0}.{1} STORED AS PARQUET AS SELECT * FROM (
        SELECT *, pmod(bitxor(bitxor({7} * {9}, rotateright({7} * {9}, 24)) * {9}, {6} * {9}), {10}) as pval
        FROM {2}.{3}
        ) tmp
        WHERE pval <= {4} * {10} and rand(unix_timestamp() + {6} + 1) <= {5}
        """.format(sample_schema, S1_name, T1_schema, T1_table, p, q1,
                   hash_num + i, T1_join_col, hash_num2, m, modval)
        create_S2_sql = """CREATE TABLE IF NOT EXISTS {0}.{1} STORED AS PARQUET AS SELECT * FROM (
        SELECT *, pmod(bitxor(bitxor({7} * {9}, rotateright({7} * {9}, 24)) * {9}, {6} * {9}), {10}) as pval
        FROM {2}.{3}
        ) tmp
        WHERE pval <= {4} * {10} and rand(unix_timestamp() + {6} + 1) <= {5}
        """.format(sample_schema, S2_name, T2_schema, T2_table, p, q2,
                   hash_num + i, T2_join_col, hash_num2, m, modval)


        #  create_S1_sql = """CREATE TABLE IF NOT EXISTS {0}.{1} STORED AS PARQUET AS SELECT * FROM (
        #  SELECT *, rand(unix_timestamp() + {6} + 1) as qval, pmod(fnv_hash({7} + {6}), 1000*1000*1000*1000) as pval
        #  FROM {2}.{3} {8}
        #  ) tmp
        #  WHERE pval <= {4} * 1000*1000*1000*1000 and qval <= {5}
        #  """.format(sample_schema, S1_name, T1_schema, T1_table, p, q1,
                   #  hash_num + i, T1_join_col, T1_where)
#
        #  create_S2_sql = """CREATE TABLE IF NOT EXISTS {0}.{1} STORED AS PARQUET AS SELECT * FROM (
        #  SELECT *, rand(unix_timestamp() + {6} + 2) as qval, pmod(fnv_hash({7} + {6}), 1000*1000*1000*1000) as pval
        #  FROM {2}.{3}
        #  ) tmp
        #  WHERE pval <= {4} * 1000*1000*1000*1000 and qval <= {5}
        #  """.format(sample_schema, S2_name, T2_schema, T2_table, p, q2,
                   #  hash_num + i, T2_join_col)

        cur.execute(create_S1_sql)
        cur.execute(create_S2_sql)
        conn.commit()

        cur.execute("COMPUTE STATS {}.{}".format(sample_schema, S1_name))
        cur.execute("COMPUTE STATS {}.{}".format(sample_schema, S2_name))

    # add metadata
    create_metatable_sql = """CREATE TABLE IF NOT EXISTS {}.meta (t1_name STRING, t2_name STRING, agg STRING,
    t1_join_col STRING, t2_join_col STRING, t1_agg_col STRING, t1_where_col STRING,
    p DOUBLE, q DOUBLE, ts TIMESTAMP ) STORED AS PARQUET
    """.format(sample_schema)
    cur.execute(create_metatable_sql)

    ts = int(time.time())
    insert_meta = """INSERT INTO TABLE {}.meta VALUES ('{}','{}','{}','{}','{}','{}',{},{},{},now())""".format(
        sample_schema, T1_table, T2_table, 'all', T1_join_col, T2_join_col,
        'N/A', 'NULL', p, q1)
    cur.execute(insert_meta)
    cur.close()


def create_sample(num_rows,
                  num_keys,
                  leftDist,
                  rightDist,
                  type,
                  sample_schema,
                  isCentralized=True):

    np.random.seed(int(time.time()))
    d = ''
    if isCentralized:
        d = 'cent'
    else:
        d = 'dec'

    T1_name = "t_{0}n_{1}k_{2}_{3}".format(num_rows, num_keys, leftDist, 1)
    T2_name = "t_{0}n_{1}k_{2}_{3}".format(num_rows, num_keys, rightDist, 2)

    S1_name = "s_{0}n_{1}k_{2}_{3}_{4}_{5}".format(num_rows, num_keys,
                                                   leftDist, type, d, 1)
    S2_name = "s_{0}n_{1}k_{2}_{3}_{4}_{5}".format(num_rows, num_keys,
                                                   rightDist, type, d, 2)

    a_v = np.zeros((num_keys, 3))
    b_v = np.zeros((num_keys, 3))
    mu_v = np.zeros((num_keys, 3))
    var_v = np.zeros((num_keys, 3))

    all_keys = np.arange(1, num_keys + 1)
    a_v[:, 0] = all_keys
    b_v[:, 0] = all_keys
    mu_v[:, 0] = all_keys
    var_v[:, 0] = all_keys

    conn = impaladb.connect(impala_host, impala_port)
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS {0}".format(sample_schema))
    T1_group_by_count_sql = """SELECT col1, COUNT(*), avg(col2), variance(col2) FROM {0}.{1} GROUP BY col1;
    """.format(parquet_schema, T1_name)
    cur.execute(T1_group_by_count_sql)
    results = cur.fetchall()

    for row in results:
        col1 = row[0]
        cnt = row[1]
        mean = row[2]
        var = row[3]
        a_v[col1 - 1, 1] = cnt
        mu_v[col1 - 1, 1] = mean
        if var is None:
            var = 0
        var_v[col1 - 1, 1] = var
    a_v[:, 2] = a_v[:, 1]**2
    mu_v[:, 2] = mu_v[:, 1]**2

    p = 0

    if isCentralized:
        T2_group_by_count_sql = """SELECT col1, COUNT(*) FROM {0}.{1} GROUP BY col1;
        """.format(parquet_schema, T2_name)
        cur.execute(T2_group_by_count_sql)
        results = cur.fetchall()

        for row in results:
            col1 = row[0]
            cnt = row[1]
            b_v[col1 - 1, 1] = cnt
        b_v[:, 2] = b_v[:, 1]**2

        if type == 'count':
            sum1 = sum(a_v[:, 2] * b_v[:, 2] - a_v[:, 2] * b_v[:, 1] -
                       a_v[:, 1] * b_v[:, 2] + a_v[:, 1] * b_v[:, 1])
            sum2 = sum(a_v[:, 1] * b_v[:, 1])
            val = e1 * e2 * sum1 / sum2
            if val > 0:
                val = math.sqrt(val)
            p = min([1, max([e1, e2, val])])
        elif type == 'sum':
            a_star = max(a_v[:, 1])
            #  % calculate first sum in the formula
            #  sum1 = sum( a_v(:,3) .* mu_v(:,3) .* b_v(:,3) );
            #
            #  % second sum
            #  sum2 = sum( a_v(:,3) .* mu_v(:,3) .* b_v(:,2) );
            #
            #  % third.. and so on
            #  sum3 = sum( a_v(:,2) .* (mu_v(:,3) + var_v(:,2)) .* b_v(:,3) );
            #
            #  sum4 = sum( a_v(:,2) .* (mu_v(:,3) + var_v(:,2)) .* b_v(:,2) );
            #
            #  sum5 = sum( a_v(:,2) .* (mu_v(:,3) + var_v(:,2)) .* b_v(:,2) );
            #
            #  % calculate the value
            #  val = e1 * e2 * (sum1 - sum2 - sum3 + sum4) / sum5;
            #  val = sqrt(val);
            sum1 = sum(a_v[:, 2] * mu_v[:, 2] * b_v[:, 2])
            sum2 = sum(a_v[:, 2] * mu_v[:, 2] * b_v[:, 1])
            sum3 = sum(a_v[:, 1] * (mu_v[:, 2] + var_v[:, 1]) * b_v[:, 2])
            sum4 = sum(a_v[:, 1] * (mu_v[:, 2] + var_v[:, 1]) * b_v[:, 1])
            sum5 = sum(a_v[:, 1] * (mu_v[:, 2] + var_v[:, 1]) * b_v[:, 1])
            val = e1 * e2 * (sum1 - sum2 - sum3 + sum4) / sum5
            val = math.sqrt(val)
            p = min([1, max([e1, e2, val])])
    else:
        T2_group_by_count_sql = """SELECT COUNT(*) FROM {0}.{1};
        """.format(parquet_schema, T2_name)
        cur.execute(T2_group_by_count_sql)
        results = cur.fetchall()
        n_b = 0
        a_star = max(a_v[:, 1])
        for row in results:
            n_b = row[0]

        if type == 'count':
            #  sum1 = e1 * e2 * (a_star^2 * n_b^2 + a_star^2 * n_b + a_star * n_b^2 + a_star * n_b);
            #  sum2 = a_star * n_b;
            sum1 = e1 * e2 * (a_star**2 * n_b**2 + a_star**2 * n_b +
                              a_star * n_b**2 + a_star * n_b)
            sum2 = a_star * n_b
            val = math.sqrt(sum1 / sum2)
            p = min([1, max([e1, e2, val])])
        elif type == 'sum':
            v = np.zeros((num_keys, 3))
            v[:, 0] = all_keys
            #  v(:,2) = a_v(:,3) .* mu_v(:,3);
            #  v(:,3) = a_v(:,2) .* (mu_v(:,3) + var_v(:,2));
            #  [m1 v1] = max(v(:,2));
            #  [m2 v2] = max(v(:,3));
            v[:, 1] = a_v[:, 2] * mu_v[:, 2]
            v[:, 2] = a_v[:, 1] * (mu_v[:, 2] + var_v[:, 1])
            v1 = np.argmax(v[:, 1])
            v2 = np.argmax(v[:, 2])
            a_vi = [a_v[v1, 1], a_v[v2, 1]]
            mu_vi = [mu_v[v1, 1], mu_v[v2, 1]]
            var_vi = [var_v[v1, 2], var_v[v2, 1]]
            #  quadratic equation for h1(p) - h2(p) = 0
            eq = [
                h_p2(e1, e2, a_vi[0], n_b, mu_vi[0], var_vi[0]) -
                h_p2(e1, e2, a_vi[1], n_b, mu_vi[1], var_vi[1]),
                h_p(e1, e2, a_vi[0], n_b, mu_vi[0], var_vi[0]) -
                h_p(e1, e2, a_vi[1], n_b, mu_vi[1], var_vi[1]),
                h_const(e1, e2, a_vi[0], n_b, mu_vi[0], var_vi[0]) -
                h_const(e1, e2, a_vi[1], n_b, mu_vi[1], var_vi[1])
            ]
            r = np.roots(eq)
            p1 = 0
            p2 = 0
            if len(r) == 2:
                p1 = r[0]
                p2 = r[1]

            #  % calculate first sum in the formula
            #  sum1 = sum(a_v(v1, 3) .* mu_v(v1, 3) .* n_b^2);
            #
            #  % second sum
            #  sum2 = sum(a_v(v1, 3) .* mu_v(v1, 3) .* n_b);
            #
            #  % third.. and so on
            #  sum3 = sum(a_v(v1, 2) .* (mu_v(v1, 3) + var_v(v1, 2)) .* n_b^2);
            #
            #  sum4 = sum(a_v(v1, 2) .* (mu_v(v1, 3) + var_v(v1, 2)) .* n_b);
            #
            #  sum5 = sum(a_v(v1, 2) .* (mu_v(v1, 3) + var_v(v1, 2)) .* n_b);
            #
            #  % calculate the value
            #  val = e1 * e2 * (sum1 - sum2 - sum3 + sum4) / sum5;
            #  val = sqrt(val);
            #
            #  p3 = min([1 max([e1 e2 val])]);
            #
            #  sum1 = sum(a_v(v2, 3) .* mu_v(v2, 3) .* n_b^2);
            #  sum2 = sum(a_v(v2, 3) .* mu_v(v2, 3) .* n_b);
            #  sum3 = sum(a_v(v2, 2) .* (mu_v(v2, 3) + var_v(v2, 2)) .* n_b^2);
            #  sum4 = sum(a_v(v2, 2) .* (mu_v(v2, 3) + var_v(v2, 2)) .* n_b);
            #  sum5 = sum(a_v(v2, 2) .* (mu_v(v2, 3) + var_v(v2, 2)) .* n_b);
            #  val = e1 * e2 * (sum1 - sum2 - sum3 + sum4) / sum5;
            #  val = sqrt(val);
            #
            #  p4 = min([1 max([e1 e2 val])]);
            #
            #  p5 = max(e1, e2);
            #
            #  pval = [p1; p2; p3; p4; p5];
            #  pval(1,2) = max([h(p1, e1, e2, a_vi(1), n_b, mu_vi(1), var_vi(1)) h(p1, e1, e2, a_vi(2), n_b, mu_vi(2), var_vi(2))]);
            #  pval(2,2) = max([h(p2, e1, e2, a_vi(1), n_b, mu_vi(1), var_vi(1)) h(p2, e1, e2, a_vi(2), n_b, mu_vi(2), var_vi(2))]);
            #  pval(3,2) = max([h(p3, e1, e2, a_vi(1), n_b, mu_vi(1), var_vi(1)) h(p3, e1, e2, a_vi(2), n_b, mu_vi(2), var_vi(2))]);
            #  pval(4,2) = max([h(p4, e1, e2, a_vi(1), n_b, mu_vi(1), var_vi(1)) h(p4, e1, e2, a_vi(2), n_b, mu_vi(2), var_vi(2))]);
            #  pval(5,2) = max([h(p5, e1, e2, a_vi(1), n_b, mu_vi(1), var_vi(1)) h(p5, e1, e2, a_vi(2), n_b, mu_vi(2), var_vi(2))]);
            #
            #  pval(find(~isreal(pval(:,1))), :) = [];
            #  pval(find(pval(:,1) < max(e1, e2)), :) = [];
            #
            #  [m i] = min(pval(:,2));
            #
            #  p = pval(i,1);
            sum1 = a_v[v1, 2] * mu_v[v1, 2] * n_b**2
            sum2 = a_v[v1, 2] * mu_v[v1, 2] * n_b
            sum3 = a_v[v1, 1] * (mu_v[v1, 2] + var_v[v1, 1]) * n_b**2
            sum4 = a_v[v1, 1] * (mu_v[v1, 2] + var_v[v1, 1]) * n_b
            sum5 = a_v[v1, 1] * (mu_v[v1, 2] + var_v[v1, 1]) * n_b
            val = e1 * e2 * (sum1 - sum2 - sum3 + sum4) / sum5
            val = math.sqrt(val)
            p3 = min([1, max([e1, e2, val])])

            sum1 = a_v[v2, 2] * mu_v[v2, 2] * n_b**2
            sum2 = a_v[v2, 2] * mu_v[v2, 2] * n_b
            sum3 = a_v[v2, 1] * (mu_v[v2, 2] + var_v[v2, 1]) * n_b**2
            sum4 = a_v[v2, 1] * (mu_v[v2, 2] + var_v[v2, 1]) * n_b
            sum5 = a_v[v2, 1] * (mu_v[v2, 2] + var_v[v2, 1]) * n_b
            val = e1 * e2 * (sum1 - sum2 - sum3 + sum4) / sum5
            val = math.sqrt(val)
            p4 = min([1, max([e1, e2, val])])

            p5 = max([e1, e2])

            pval = np.zeros((5, 2))
            pval[0, 0] = p1
            pval[1, 0] = p2
            pval[2, 0] = p3
            pval[3, 0] = p4
            pval[4, 0] = p5
            pval[0, 1] = max([
                h(p1, e1, e2, a_vi[0], n_b, mu_vi[0], var_vi[0]),
                h(p1, e1, e2, a_vi[1], n_b, mu_vi[1], var_vi[1])
            ])
            pval[1, 1] = max([
                h(p2, e1, e2, a_vi[0], n_b, mu_vi[0], var_vi[0]),
                h(p2, e1, e2, a_vi[1], n_b, mu_vi[1], var_vi[1])
            ])
            pval[2, 1] = max([
                h(p3, e1, e2, a_vi[0], n_b, mu_vi[0], var_vi[0]),
                h(p3, e1, e2, a_vi[1], n_b, mu_vi[1], var_vi[1])
            ])
            pval[3, 1] = max([
                h(p4, e1, e2, a_vi[0], n_b, mu_vi[0], var_vi[0]),
                h(p4, e1, e2, a_vi[1], n_b, mu_vi[1], var_vi[1])
            ])
            pval[4, 1] = max([
                h(p5, e1, e2, a_vi[0], n_b, mu_vi[0], var_vi[0]),
                h(p5, e1, e2, a_vi[1], n_b, mu_vi[1], var_vi[1])
            ])
            check_real = np.isreal(pval[:, 0])
            pval = np.delete(pval, np.argwhere(check_real is False), 0)
            pval = np.delete(pval, np.argwhere(pval[:, 0] < max([e1, e2])), 0)
            m = np.argmin(pval[:, 1])
            p = pval[m, 0]
    q1 = e1 / p
    q2 = e2 / p
    ts = int(time.time())

    create_S1_sql = """CREATE TABLE {0}.{1} STORED AS PARQUET AS SELECT col1, col2, col3, col4 FROM (
    SELECT col1, col2, col3, col4, rand(unix_timestamp()) as qval, pmod(fnv_hash(col1 + {6}), 100000) as pval
    FROM {2}.{3}
    ) tmp
    WHERE pval <= {4} * 100000 and qval <= {5}
    """.format(sample_schema, S1_name, parquet_schema, T1_name, p, q1, ts)

    create_S2_sql = """CREATE TABLE {0}.{1} STORED AS PARQUET AS SELECT col1, col2, col3, col4 FROM (
    SELECT col1, col2, col3, col4, rand(unix_timestamp()) as qval, pmod(fnv_hash(col1 + {6}), 100000) as pval
    FROM {2}.{3}
    ) tmp
    WHERE pval <= {4} * 100000 and qval <= {5}
    """.format(sample_schema, S2_name, parquet_schema, T2_name, p, q2, ts)

    cur.execute(create_S1_sql)
    cur.execute(create_S2_sql)


def create_cent_stratified_sample_pair_from_impala(host,
                                                   port,
                                                   T1_schema,
                                                   T1_table,
                                                   T1_join_col,
                                                   T1_agg_col,
                                                   T1_group_col,
                                                   T2_schema,
                                                   T2_table,
                                                   T2_join_col,
                                                   sample_schema,
                                                   agg_type,
                                                   key_t,
                                                   row_t,
                                                   num_sample,
                                                   overwrite=False):
    # seed the rng
    hash_val = int(
        hashlib.sha1(str(uuid.uuid1().bytes).encode()).hexdigest(), 16) % (10**
                                                                           8)
    np.random.seed(int(time.time()) + hash_val)

    conn = impaladb.connect(host, port)
    cur = conn.cursor()

    T1_join_key_count = 0
    T2_join_key_count = 0

    cur.execute("SELECT MAX({0}) FROM {1}.{2}".format(
        T1_group_col, T1_schema, T1_table))
    res = cur.fetchone()
    num_group = res[0]
    g_v = np.zeros((num_group+1, 3))
    K_major = 0
    K_minor = 0
    N_major = 0
    N_minor = 0
    major_group = []
    minor_group = []
    cur.execute("SELECT {3}, COUNT(*), COUNT(DISTINCT {0}) FROM {1}.{2} GROUP BY {3} ".format(
        T1_join_col, T1_schema, T1_table, T1_group_col))
    while True:
        results = cur.fetchmany(fetch_size)
        if not results:
            break

        for row in results:
            col1 = row[0]
            cnt = row[1]
            cnt2 = row[2]
            g_v[col1, 0] = cnt
            g_v[col1, 1] = cnt2 
            if cnt2 >= key_t:
                K_major = K_major + 1
                N_major = N_major + cnt
                major_group.append(col1)
            else:
                K_minor = K_minor + 1
                N_minor = N_minor + cnt
                minor_group.append(col1)

    print((K_major, K_minor, N_major, N_minor))
    # check budget
    if row_t * K_major > e1 * N_major:
        print("Budget for majority exceeded")
        return
    if row_t * K_minor > e1 * N_minor:
        print("Budget for minority exceeded")
        return

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

    b_v = np.zeros((num_keys, 2))
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

    keys = np.arange(1, num_keys + 1)

    # calculate left-over budget
    e_left_major = (e1 * N_major - row_t * K_major) / N_major
    e_left_minor = (e1 * N_minor - row_t * K_minor) / N_minor

    val1 = 0
    val2 = 0
    ratio = 1

    for i in range(0, num_group + 1):
        if i in major_group:
            g_v[i,2] = (row_t / g_v[i, 0]) + e_left_major
        elif i in minor_group:
            g_v[i,2] = (row_t / g_v[i, 0]) + e_left_minor
        else:
            continue

        a_v = np.zeros((num_keys, 2))
        mu_v = np.zeros((num_keys, 2))
        var_v = np.zeros((num_keys, 2))

        T1_group_by_count_sql = """SELECT {0}, COUNT(*), avg({1}), variance({1}) FROM {2}.{3} WHERE {4} = {5} GROUP BY {0};
        """.format(T1_join_col, T1_agg_col, T1_schema, T1_table, T1_group_col, i)
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
        mu_v = np.nan_to_num(mu_v)
        var_v = np.nan_to_num(var_v)

        if agg_type == 'count':
            sum1 = sum(a_v[:, 1] * b_v[:, 1] - a_v[:, 1] * b_v[:, 0] -
                       a_v[:, 0] * b_v[:, 1] + a_v[:, 0] * b_v[:, 0])
            sum2 = sum(a_v[:, 0] * b_v[:, 0])
            val1 = val1 + (ratio * e1 * e2 * sum1)
            val2 = val2 + (ratio * sum2)
        elif agg_type == 'sum':
            sum1 = sum(a_v[:, 1] * mu_v[:, 1] * b_v[:, 1])
            sum2 = sum(a_v[:, 1] * mu_v[:, 1] * b_v[:, 0])
            sum3 = sum(a_v[:, 0] * (mu_v[:, 1] + var_v[:, 0]) * b_v[:, 1])
            sum4 = sum(a_v[:, 0] * (mu_v[:, 1] + var_v[:, 0]) * b_v[:, 0])
            sum5 = sum(a_v[:, 0] * (mu_v[:, 1] + var_v[:, 0]) * b_v[:, 0])
            val1 = val1 + ratio * e1 * e2 * (sum1 - sum2 - sum3 + sum4)
            val2 = val2 + ratio * sum5
        elif agg_type == 'avg':
            A_denom = sum(a_v[:, 0] * mu_v[:, 0] * b_v[:, 0])**2
            A1 = sum(a_v[:, 0] *
                     (mu_v[:, 1] + var_v[:, 0]) * b_v[:, 0]) / A_denom
            A2 = sum(a_v[:, 1] * mu_v[:, 1] * b_v[:, 1]) / A_denom
            A3 = sum(a_v[:, 1] * mu_v[:, 1] * b_v[:, 0]) / A_denom
            A4 = sum(a_v[:, 0] * (mu_v[:, 1] + var_v[:, 0]) * b_v[:, 1])
            A = A1 + A2 - A3 - A4

            B_denom = sum(a_v[:, 0] * b_v[:, 0]) * sum(
                a_v[:, 0] * mu_v[:, 0] * b_v[:, 0])
            B1 = 1 / sum(a_v[:, 0] * b_v[:, 0])
            B2 = sum(a_v[:, 1] * mu_v[:, 0] * b_v[:, 1]) / B_denom
            B3 = sum(a_v[:, 1] * mu_v[:, 0] * b_v[:, 0]) / B_denom
            B4 = sum(a_v[:, 0] * mu_v[:, 0] * b_v[:, 1]) / B_denom
            B = B1 + B2 - B3 - B4

            C_denom = sum(a_v[:, 0] * b_v[:, 0])**2
            C1 = sum(a_v[:, 0] * b_v[:, 0]) / C_denom
            C2 = sum(a_v[:, 1] * b_v[:, 1]) / C_denom
            C3 = sum(a_v[:, 1] * b_v[:, 0]) / C_denom
            C4 = sum(a_v[:, 0] * b_v[:, 1]) / C_denom
            C = C1 + C2 - C3 - C4

            D = (1 / e1 * e2) * (A1 - 2 * B1 + C1)
            v1 = A - (2 * B) + C
            v2 = D

            val1 = val1 + (ratio * v1)
            val2 = val2 + (ratio * v2)
        else:
            print("Unsupported operation")
            return

    if val1 <= 0 and val2 > 0:
        p = max(e1, e2)
    elif val1 > 0 and val2 <= 0:
        p = 1
    elif val1 > 0 and val2 > 0:
        val = math.sqrt(val1 / val2)
        p = min([1, max([e1, e2, val])])
    else:
        p_minus = max(e1, e2)
        p_plus = 1
        pval1 = (1 / p_minus) * val1 + p_minus * val2
        pval2 = (1 / p_plus) * val2 + p_plus * val2
        if pval1 < pval2:
            p = p_minus
        else:
            p = p_plus
    
    print(g_v)
    print(p)

    # hash_num = np.random.randint(1000 * 1000 * 1000)
    # for i in range(1, num_sample + 1):
    #     S1_name = "s__{}__{}__{}__{}__{}__{}__{}__{}__{}".format(
    #         T1_table, T2_table, T1_join_col, T1_agg_col, T1_group_col,
    #         agg_type, K, i, 1)
    #     S2_name = "s__{}__{}__{}__{}__{}__{}__{}".format(
    #         T1_table, T2_table, T2_join_col, agg_type, K, i, 2)

    #     if overwrite:
    #         cur.execute("DROP TABLE IF EXISTS {}.{}".format(
    #             sample_schema, S1_name))
    #         cur.execute("DROP TABLE IF EXISTS {}.{}".format(
    #             sample_schema, S2_name))
    #         conn.commit()

    #     # create tables first for S1
    #     create_S1 = """
    #     CREATE TABLE IF NOT EXISTS {}.{} LIKE {}.{} STORED AS PARQUET
    #     """.format(sample_schema, S1_name, T1_schema, T1_table)
    #     cur.execute(create_S1)
    #     # add pval, qval columns
    #     add_column_S1 = """
    #     ALTER TABLE {}.{} ADD COLUMNS (qval DOUBLE, pval BIGINT)
    #     """.format(sample_schema, S1_name)
    #     cur.execute(add_column_S1)

    #     # sample data per group
    #     for group_val in range(start_val, end_val + 1):
    #         all_sample = group_info_map[group_val]
    #         if all_sample:
    #             sql = """
    #                 INSERT INTO TABLE {0}.{1} SELECT *, 1 as qval, 0 as pval
    #                 FROM {2}.{3} WHERE {4} = {5}
    #             """.format(sample_schema, S1_name, T1_schema, T1_table,
    #                        T1_group_col, group_val)
    #         else:
    #             sql = """INSERT INTO TABLE {0}.{1} SELECT * FROM (
    #             SELECT *, rand(unix_timestamp() + {6} + 1) as qval, pmod(fnv_hash({7} + {6}), 1000*1000*1000*1000) as pval
    #             FROM {2}.{3} WHERE {8} = {9}
    #             ) tmp
    #             WHERE pval <= {4} * 1000*1000*1000*1000 and qval <= {5}
    #             """.format(sample_schema, S1_name, T1_schema, T1_table, p, q1,
    #                        hash_num + i, T1_join_col, T1_group_col, group_val)
    #         cur.execute(sql)

    #     # sample S2 as usual
    #     create_S2_sql = """CREATE TABLE IF NOT EXISTS {0}.{1} STORED AS PARQUET AS SELECT * FROM (
    #     SELECT *, rand(unix_timestamp() + {6} + 2) as qval, pmod(fnv_hash({7} + {6}), 1000*1000*1000*1000) as pval
    #     FROM {2}.{3}
    #     ) tmp
    #     WHERE pval <= {4} * 1000*1000*1000*1000 and qval <= {5}
    #     """.format(sample_schema, S2_name, T2_schema, T2_table, p, q2,
    #                hash_num + i, T2_join_col)

    #     cur.execute(create_S2_sql)
    #     conn.commit()

    #     cur.execute("COMPUTE STATS {}.{}".format(sample_schema, S1_name))
    #     cur.execute("COMPUTE STATS {}.{}".format(sample_schema, S2_name))

    # # add metadata
    # create_metatable_sql = """CREATE TABLE IF NOT EXISTS {}.meta (t1_name STRING, t2_name STRING, agg STRING,
    # t1_join_col STRING, t2_join_col STRING, t1_agg_col STRING, t1_group_col STRING,
    # p DOUBLE, q DOUBLE, ts TIMESTAMP ) STORED AS PARQUET
    # """.format(sample_schema)
    # cur.execute(create_metatable_sql)

    # create_grp_cnt_sql = """
    #     CREATE TABLE IF NOT EXISTS {0}.{1} STORED AS PARQUET AS SELECT * FROM {0}.{2}
    # """.format(sample_schema, grp_cnt_table, grp_cnt_temp)
    # cur.execute(create_grp_cnt_sql)
    # cur.execute("DROP TABLE IF EXISTS {}.{}".format(sample_schema,
    #                                                 grp_cnt_temp))

    # ts = int(time.time())
    # insert_meta = """INSERT INTO TABLE {}.meta VALUES
    # ('{}','{}','{}','{}','{}','{}','{}',{},{},now())""".format(
    #     sample_schema, T1_table, T2_table, agg_type, T1_join_col, T2_join_col,
    #     T1_agg_col, T1_group_col, p, q1)
    # cur.execute(insert_meta)
    # conn.commit()
    # cur.close()


def create_sample_pair_count_with_cond(
        T1_rows,
        T1_keys,
        T2_rows,
        T2_keys,
        leftDist,
        rightDist,
        type,  # dist. of conditon var
        rel_type,
        num_pred_val,
        num_samples,
        isCentralized=True):
    np.random.seed(int(time.time() + threading.get_ident()) % (2**32))
    dir = data_path + 'our_samples/with_cond/'
    d = ''
    if isCentralized:
        d = 'centralized'
    else:
        d = 'decentralized'
    e1 = 0.01
    e2 = 0.01

    sample_dir = dir + d + '/'
    pathlib.Path(sample_dir).mkdir(parents=True, exist_ok=True)

    T1_name = raw_data_path + "/t_{0}n_{1}k_{2}_{3}_{4}.csv".format(
        T1_rows, T1_keys, leftDist, rel_type, 1)
    T2_name = raw_data_path + "/t_{0}n_{1}k_{2}_{3}.csv".format(
        T2_rows, T2_keys, rightDist, 2)

    num_keys = max([T1_keys, T2_keys])

    # read table files
    T1_df = pd.read_csv(T1_name, sep='|', header=None, usecols=[0, 1, 2])
    # drop dummy col
    #  T1_df = T1_df.drop(columns=[3])
    T1 = T1_df.values
    T1 = T1.astype(int)

    T2_df = pd.read_csv(T2_name, sep='|', header=None, usecols=[0, 1, 2])
    #  T2_df = T2_df.drop(columns=[3])
    T2 = T2_df.values
    T2 = T2.astype(int)

    a_v = np.zeros((num_keys, num_pred_val, 2))
    b_v = np.zeros((num_keys, 3))
    mu_v = np.zeros((num_keys, num_pred_val, 2))
    var_v = np.zeros((num_keys, num_pred_val, 1))

    all_keys = np.arange(1, num_keys + 1)
    #  a_v[:, 0] = all_keys
    b_v[:, 0] = all_keys
    #  mu_v[:, 0] = all_keys
    #  var_v[:, 0] = all_keys

    # get mean and var
    gr = T1_df.groupby([0, 2])
    keys = np.array(list(gr.groups.keys()))
    counts = np.array(gr.count().values)
    means = np.array(gr[1].mean().values)
    vars = np.array(np.nan_to_num(gr[1].var().values))

    a_v[keys[:, 0].astype(int) - 1, keys[:, 1].astype(int), 0] = counts[:, 0]
    a_v[:, :, 1] = a_v[:, :, 0]**2

    mu_v[keys[:, 0].astype(int) - 1, keys[:, 1].astype(int), 0] = means
    mu_v[:, :, 1] = mu_v[:, :, 0]**2

    var_v[keys[:, 0].astype(int) - 1, keys[:, 1].astype(int), 0] = vars

    if isCentralized:
        # get group count for T2
        counts = np.array(np.unique(T2[:, 0], return_counts=True)).T
        b_v[counts[:, 0].astype(int) - 1, 1] = counts[:, 1]
        b_v[:, 2] = b_v[:, 1]**2

        sum1 = 0
        sum2 = 0

        if type == 'uniform':
            for i in range(0, num_pred_val):
                #  a_temp = np.sum(a_v[:, i, :], axis=1)
                a_temp = a_v[:, i, :]
                sum1 += sum(a_temp[:, 1] * b_v[:, 2] -
                            a_temp[:, 1] * b_v[:, 1] -
                            a_temp[:, 0] * b_v[:, 2] +
                            a_temp[:, 0] * b_v[:, 1])
                sum2 += sum(a_temp[:, 0] * b_v[:, 1])
            val = math.sqrt(e1 * e2 * sum1 / sum2)
            p = min([1, max([e1, e2, val])])
        elif type == 'identical':
            # get group count for C
            counts = np.array(np.unique(T1[:, 2], return_counts=True)).T
            c_v = np.zeros((num_pred_val, 1))
            #  c_v[counts[:, 0].astype(int), 1] = counts[:, 1]
            c_v[counts[:, 0], 0] = counts[:, 1]
            #  print(c_v.T)
            c_total = np.sum(c_v)
            for i in range(0, num_pred_val):
                #  a_temp = np.sum(a_v[:, i, :], axis=1)
                a_temp = a_v[:, i, :]
                c = c_v[i] / c_total
                sum1 += c * sum(a_temp[:, 1] * b_v[:, 2] - a_temp[:, 1] *
                                b_v[:, 1] - a_temp[:, 0] * b_v[:, 2] +
                                a_temp[:, 0] * b_v[:, 1])
                sum2 += c * sum(a_temp[:, 0] * b_v[:, 1])
            val = math.sqrt(e1 * e2 * sum1 / sum2)
            p = min([1, max([e1, e2, val])])
        else:
            print('Unsupported type: {}'.format(type))
            raise ValueError
    else:
        print("Decentralized not supported yet")
        raise ValueError

    print("sum1 = {}, sum2 = {}".format(sum1, sum2))

    q1 = e1 / p
    q2 = e2 / p

    print("p = {:.3f}, q = {:.3f}".format(p, q1))

    dir = "{}/{}n_{}k/{}_{}/{}_{}".format(sample_dir, T1_rows, T1_keys,
                                          leftDist, rightDist, type, rel_type)
    pathlib.Path(dir).mkdir(parents=True, exist_ok=True)

    # clear resources
    del a_v, b_v, mu_v, var_v, gr, keys, means, vars
    gc.collect()

    T1_extra = np.zeros((T1_rows, 2))
    T2_extra = np.zeros((T2_rows, 2))

    T1_new = np.hstack((T1, T1_extra))
    T2_new = np.hstack((T2, T2_extra))

    print("Starting to write {} samples @ {} in: {}".format(
        num_samples, str(datetime.datetime.now()), dir),
          flush=True)

    start = time.time()

    for s in range(1, num_samples + 1):

        S1_name = "{}/s1_{}.npy".format(dir, s)
        S2_name = "{}/s2_{}.npy".format(dir, s)
        if os.path.exists(S1_name) and os.path.exists(S2_name):
            #  print("Samples (#{}) already exist".format(s))
            continue

        # generate random key permutation
        key_perm = np.random.permutation(num_keys * 10) + 1
        hash_col_idx = 3

        T1_perm = key_perm[T1[:, 0].astype(int) - 1]
        #  T1_perm.shape = (T1_rows, 1)

        T2_perm = key_perm[T2[:, 0].astype(int) - 1]
        #  T2_perm.shape = (T2_rows, 1)

        #  T1_new = np.hstack((T1, T1_perm))
        #  T2_new = np.hstack((T2, T2_perm))
        T1_new[:, hash_col_idx] = T1_perm
        T2_new[:, hash_col_idx] = T2_perm

        S1 = T1_new[np.where(T1_new[:, hash_col_idx] %
                             (num_keys + 1) <= (p * num_keys))]
        S2 = T2_new[np.where(T2_new[:, hash_col_idx] %
                             (num_keys + 1) <= (p * num_keys))]

        S1_rows = np.size(S1, 0)
        S2_rows = np.size(S2, 0)

        prob_col_idx = 4
        #  S1 = np.hstack((S1, np.random.rand(S1_rows, 1)))
        #  S2 = np.hstack((S2, np.random.rand(S2_rows, 1)))
        S1[:, prob_col_idx] = np.random.rand(S1_rows)
        S2[:, prob_col_idx] = np.random.rand(S2_rows)

        S1 = S1[np.where(S1[:, prob_col_idx] <= q1)]
        S2 = S2[np.where(S2[:, prob_col_idx] <= q2)]

        S1 = S1[:, 0:3]
        S2 = S2[:, 0:3]

        S1_data = {}
        S1_data['sample'] = S1
        S1_data['p'] = p
        S1_data['q'] = q1

        S2_data = {}
        S2_data['sample'] = S2
        S2_data['p'] = p
        S2_data['q'] = q2

        np.save(S1_name, S1_data)
        np.save(S2_name, S2_data)

        del S1, S2, key_perm, T1_perm, T2_perm
        gc.collect()

    end = time.time()
    print("Sample creation done (took {} s) in: {}".format(
        str(end - start), dir),
          flush=True)
    return True


def h(p, e1, e2, a, b, mu, sig):
    return (1 / e2 - 1 / p) * a**2 * mu**2 * b + (1 / e1 - 1 / p) * a * (
        mu**2 + sig) * b**2 + (p / (e1 * e2) - 1 / e1 - 1 / e2 + 1 / p) * a * (
            mu**2 + sig) * b + (1 / p - 1) * a**2 * mu**2 * b**2


def h_p2(e1, e2, a, b, mu, sig):
    return (a * (mu**2 + sig) * b**2) / (e1 * e2)


def h_p(e1, e2, a, b, mu, sig):
    return (1 /
            e2) * a**2 * mu**2 * b + (1 / e1) * a * (mu**2 + sig) * b**2 - (
                1 / e1 + 1 / e2) * a * (mu**2 + sig) * b - a**2 * mu**2 * b**2


def h_const(e1, e2, a, b, mu, sig):
    return a * (mu**2 +
                sig) * b + a**2 + mu**2 + b**2 - a**2 * mu**2 * b - a * (
                    mu**2 + sig) * b**2
