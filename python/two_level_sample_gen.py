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
import scipy
import ibis

hdfs_dir = "/tmp/approxjoin"
data_path = "/home/dyoon/work/approxjoin_data/"
raw_data_path = "/media/hdd/approxjoin_test/synthetic/data"
text_schema = "approxjoin_text"
parquet_schema = "approxjoin_parquet"
impala_host = "cp-2"
impala_port = 21050
fetch_size = 1000000
e1 = 0.02
e2 = 0.02
m = 1540483477
modval = 2 ** 32

# https://stackoverflow.com/questions/22699756/python-version-of-ismember-with-rows-and-index
def asvoid(arr):
    """
    View the array as dtype np.void (bytes)
    This views the last axis of ND-arrays as bytes so you can perform comparisons on
    the entire row.
    http://stackoverflow.com/a/16840350/190597 (Jaime, 2013-05)
    Warning: When using asvoid for comparison, note that float zeros may compare UNEQUALLY
    >>> asvoid([-0.]) == asvoid([0.])
    array([False], dtype=bool)
    """
    arr = np.ascontiguousarray(arr)
    return arr.view(np.dtype((np.void, arr.dtype.itemsize * arr.shape[-1])))


def in1d_index(a, b):
    voida, voidb = map(asvoid, (a, b))
    return np.where(~np.in1d(voidb, voida))[0]


def sigma1(v, a_v, b_v, q):
    sigma = (
        ((1 / (q ** 2)) - 1) * (a_v[v, 0] - 1) * (b_v[v, 0] - 1)
        + (1 / q - 1) * (b_v[v, 0] - 1) * (a_v[v, 1] - a_v[v, 0] + 1)
        + (1 / q - 1) * (a_v[v, 0] - 1) * (b_v[v, 1] - b_v[v, 0] + 1)
    )
    sigma[sigma < 0] = 0
    # print(sigma)
    return sigma


def two_level_sample_func(keys, a_v, b_v, q):
    sig = sigma1(keys, a_v, b_v, q)
    # print(sig, q)
    a = sig + a_v[keys, 1] * b_v[keys, 1]
    b = 2 + q * (a_v[keys, 0] + b_v[keys, 0] - 2)
    # print(a, b)
    # print(sig.shape)
    # print(a.shape)
    # print(b.shape)
    v = np.sqrt(a * b)
    val = sum(v) ** 2
    # print("q = {}".format(q))
    # print("val = {}".format(val))
    return val


def create_two_level_sample_pair_from_impala(
    host,
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
    print_time=False,
):
    # seed the rng
    hash_val = int(hashlib.sha1(str(uuid.uuid1().bytes).encode()).hexdigest(), 16) % (
        10 ** 8
    )
    np.random.seed(int(time.time()) + hash_val)

    conn = impaladb.connect(host, port)
    cur = conn.cursor()

    T1_join_key_count = 0
    T2_join_key_count = 0
    T1_rows = 0
    T2_rows = 0

    # get # of of join keys in each table
    T1_join_key_count_sql = "SELECT MAX({0}), count(*) FROM {1}.{2}".format(
        T1_join_col, T1_schema, T1_table
    )
    cur.execute(T1_join_key_count_sql)
    results = cur.fetchall()
    for row in results:
        T1_join_key_count = row[0]
        T1_rows = row[1]
    cur.close()

    cur = conn.cursor()
    T2_join_key_count_sql = "SELECT MAX({0}), count(*) FROM {1}.{2}".format(
        T2_join_col, T2_schema, T2_table
    )
    cur.execute(T2_join_key_count_sql)
    results = cur.fetchall()
    for row in results:
        T2_join_key_count = row[0]
        T2_rows = row[1]
    cur.close()

    num_keys = max([T1_join_key_count, T2_join_key_count])

    # num_rows = 100 * 1000 * 1000
    t1 = np.zeros((T1_rows, 3))
    t2 = np.zeros((T2_rows, 3))
    a_v = np.zeros((num_keys, 2))
    b_v = np.zeros((num_keys, 2))
    mu_v = np.zeros((num_keys, 2))
    var_v = np.zeros((num_keys, 2))

    keys = np.arange(0, num_keys)
    #  a_v[:, 0] = keys
    #  b_v[:, 0] = keys
    #  mu_v[:, 0] = keys
    #  var_v[:, 0] = keys

    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS {0}".format(sample_schema))
    # T1_where = " WHERE {}".format(T1_where) if T1_where else ""
    # T1_group_by_count_sql = """SELECT {0}, COUNT(*) FROM {2}.{3} {4} GROUP BY {0};
    # """.format(T1_join_col, 'N/A', T1_schema, T1_table, T1_where)
    # cur.execute(T1_group_by_count_sql)
    T1_select = """SELECT * FROM {0}.{1};""".format(T1_schema, T1_table)
    cur.execute(T1_select)
    cnt = 0

    while True:
        results = cur.fetchmany(fetch_size)
        if not results:
            break

        for row in results:
            t1[cnt, 0] = row[0]
            t1[cnt, 1] = row[1]
            t1[cnt, 2] = row[2]
            a_v[row[0] - 1, 0] += 1
            cnt += 1
    a_v[:, 1] = a_v[:, 0] ** 2

    T2_select = """SELECT * FROM {0}.{1};""".format(T2_schema, T2_table)
    cur.execute(T2_select)
    cnt = 0

    while True:
        results = cur.fetchmany(fetch_size)
        if not results:
            break

        for row in results:
            t2[cnt, 0] = row[0]
            t2[cnt, 1] = row[1]
            t2[cnt, 2] = row[2]
            b_v[row[0] - 1, 0] += 1
            cnt += 1
    b_v[:, 1] = b_v[:, 0] ** 2

    fn = lambda q: two_level_sample_func(keys, a_v, b_v, q)
    q0 = 0.1
    bounds = ((0.0000001, 1),)
    # q_min = fminbound(fn, 0, 1)
    res = scipy.optimize.minimize(fn, q0, bounds=bounds)
    # print(res)
    # print(res.x)
    q_min = res.x[0]
    sig = sigma1(keys, a_v, b_v, q_min)
    const1 = lambda C: sum(
        C
        * np.sqrt(
            (sig + a_v[:, 1] * b_v[:, 1]) / (2 + q_min * (a_v[:, 0] + b_v[:, 0] - 2))
        )
        * (2 + q_min * (a_v[:, 0] + b_v[:, 0] - 2))
    ) - (T1_rows * e1)

    C0 = 1
    C = scipy.optimize.fsolve(const1, C0)

    sig = sigma1(keys, a_v, b_v, q_min)
    #    calculate p_v's
    p = C * np.sqrt(
        (sig + a_v[:, 1] * b_v[:, 1]) / (2 + q_min * (a_v[:, 0] + b_v[:, 0] - 2))
    )
    q = np.zeros((num_keys, 1))
    q[:, 0] = q_min

    perm_map = np.random.permutation(num_keys + 1)
    print(t1.shape)
    h1 = perm_map[t1[:, 0].astype(int)].reshape(T1_rows, 1)
    print(h1.shape)
    # h1 = np.reshape(T1_rows, 1)
    # print(h1.shape)
    t1_new = np.hstack((t1, h1))
    # t2_new = np.hstack((t2, perm_map[t2[:, 0].astype(int)].transpose()))
    p_map = p[t1[:, 0].astype(int) - 1]
    S = t1_new[np.where(np.mod(t1_new[:, 3], 100000) <= p_map * 100000)]
    print(S.shape)
    print(p_map)
    print(t1_new)
    print(S)
    S1 = []
    S1_sentry = []
    S1_c = np.zeros((num_keys))
    for i in range(0, np.size(S, 0)):
        v = S[i, 0].astype(int)
        if S1_c[v] == 0:
            S1_sentry.append(S[i, 0:3])
            S1_c[v] = 1
        else:
            S1_c[v] += 1
            if np.random.random() < (1 / S1_c[v]):
                S1_sentry.append(S[i, 0:3])

        newQ = q[v]
        if p[v] > 1 and q[v] == q_min:
            x = a_v[v, 1] + b_v[v, 1] - 2
            val = p[v] * (2 + q[v] * x)
            newQ = (val - 2) / x
            q[v] = newQ

        if np.random.random() < newQ:
            S1.append(S[i, 0:3])

    print(len(S1))
    print(len(S1_sentry))
    if len(S1) > 0:
        length = len(S1)
        idx = in1d_index(S1_sentry, S1)
        S1 = np.reshape(S1, (length, 3))
        print(idx)
        S1 = S1[idx, :]
    print(S1.shape)
    print(S1)

    dataset = pd.DataFrame(
        data={"col1": S1[:, 0], "col2": S1[:, 1], "col3": S1[:, 2]}, dtype=np.int
    )
    # dataset.to_sql("test1", conn, schema=sample_schema)
    # client = ibis.impala.connect(host="cp-4", port=21050)
    hdfs = ibis.hdfs_connect(host="cp-1", port=50070)
    client = ibis.impala.connect("cp-2", 21050, hdfs_client=hdfs)
    db = client.database(sample_schema)
    db.create_table("test3", dataset)
    # t = db["test2"]
    # t.execute()

    # print("== p ==")
    # print(p.shape)
    # print(p)

