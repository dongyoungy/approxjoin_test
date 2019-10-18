import datetime
import hashlib
import uuid
import impala.dbapi as impaladb
import numpy as np
import pandas as pd
import time


def create_two_level_sample_pair_from_impala(
    host,
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
    print_time=False,
):

    t1 = datetime.datetime.now()
    # seed the rng
    hash_val = int(hashlib.sha1(str(uuid.uuid1().bytes).encode()).hexdigest(), 16) % (
        10 ** 8
    )
    np.random.seed(int(time.time()) + hash_val)
    conn = impaladb.connect(host, port)
    cur = conn.cursor()

    num_keys = 10 * 1000 * 1000

    t1 = pd.read_sql_query("SELECT * FROM {0}.{1}".format(T1_schema, T1_table))
    t2 = pd.read_sql_query("SELECT * FROM {0}.{1}".format(T2_schema, T2_table))

