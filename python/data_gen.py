import csv
import math
import os
import random
import string
import subprocess
import threading
import time

import impala.dbapi as impaladb
import numpy as np
import scipy.stats as ss

hdfs_dir = '/tmp/approxjoin'
raw_data_path = './raw_data'
text_schema = 'approxjoin_text'
parquet_schema = 'approxjoin_parquet'
impala_host = 'cp-2'
impala_port = 21050


# call this function to create table
def create_table(num_rows, num_keys, type, n, overwrite=False):
    (table_name, data_file) = create_table_data(num_rows, num_keys, type, n,
                                                overwrite)
    hdfs_path = hdfs_dir + '/' + table_name
    load_data_to_hdfs(data_file, hdfs_path)
    create_text_table(table_name, hdfs_path)
    create_parquet_table(table_name)

    # clear raw file at the end
    if os.path.exists(data_file):
        os.remove(data_file)


def write_csv(writer, rows):
    writer.writerows(rows)


def load_data_to_hdfs(data_path, hdfs_path):
    # create dir first
    subprocess.run(['hdfs', 'dfs', '-mkdir', '-p', hdfs_path])

    # copy the local file to hdfs
    subprocess.run(['hdfs', 'dfs', '-put', data_path, hdfs_path])


def create_text_table(table_name, hdfs_path):
    conn = impaladb.connect(impala_host, impala_port)
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS {0}".format(text_schema))

    create_text_sql = """CREATE EXTERNAL TABLE IF NOT EXISTS `{0}`.`{1}` (
                      `col1`  BIGINT,
                      `col2`  INT,
                      `col3`  INT,
                      `col4`  STRING)
                      ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
                      LOCATION '{2}';
                      """.format(text_schema, table_name, hdfs_path)

    cur.execute(create_text_sql)


def create_parquet_table(table_name):
    conn = impaladb.connect(impala_host, impala_port)
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS {0}".format(parquet_schema))

    create_parquet_sql = """CREATE TABLE IF NOT EXISTS {0}.{2} STORED AS parquet AS
    SELECT * FROM {1}.{2};
    """.format(parquet_schema, text_schema, table_name)

    cur.execute(create_parquet_sql)

    compute_stat_sql = "COMPUTE STATS {0}.{1};".format(parquet_schema,
                                                       table_name)

    cur.execute(compute_stat_sql)


def create_table_data(num_rows, num_keys, type, n, overwrite=False):
    if not os.path.exists(raw_data_path):
        os.makedirs(raw_data_path)

    table_name = "t_{0}n_{1}k_{2}_{3}".format(num_rows, num_keys, type, n)
    data_file = raw_data_path + '/' + "{0}.csv".format(table_name)

    if os.path.exists(data_file) and not overwrite:
        print("Data file: '{0}' already exists".format(data_file))
        return table_name, data_file

    delim = '|'
    remaining = num_rows
    dummy_col_size = 200
    # write 500k records at a time
    default_batch_size = 500000
    # seed the rng
    np.random.seed(int(time.time()))

    # open file
    f = open(data_file, "w")

    write_thread = None
    writer = csv.writer(f, delimiter=delim)

    current_batch_size = default_batch_size

    # set random string for dummy column
    val3 = [
        ''.join(
            random.choice(string.ascii_uppercase + string.digits)
            for _ in range(dummy_col_size))
    ] * current_batch_size

    # set random permutation of keys
    keymap = np.random.permutation(num_keys) + 1

    while remaining > 0:

        if remaining < default_batch_size:
            current_batch_size = remaining
            val3 = [
                ''.join(
                    random.choice(string.ascii_uppercase + string.digits)
                    for _ in range(dummy_col_size))
            ] * current_batch_size

        if type == 'uniform':
            keys = np.random.randint(1, num_keys + 1, current_batch_size)

        elif type == 'normal':
            # from: https://stackoverflow.com/questions/37411633/
            # how-to-generate-a-random-normal-distribution-of-integers
            r = num_keys / 2
            scale = num_keys / 5
            keys = ss.truncnorm(a=(-r + 1) / scale, b=r / scale,
                                scale=scale).rvs(num_rows)
            keys = keys + r
            keys = keys.round().astype(int)

        elif type == 'powerlaw':
            alpha = -2.5
            minv = 1
            maxv = num_keys
            rand_keys = np.array(np.random.random(size=current_batch_size))
            keys = ((maxv**(alpha + 1) - minv**(alpha + 1)) * rand_keys +
                    minv**(alpha + 1))**(1 / (alpha + 1))
            keys = [math.floor(k) for k in keys]

        else:
            print("Unsupported type: {0}".format(type))
            return

        # generate data for two value columns
        val1 = np.random.normal(100, 25, current_batch_size).astype(int)
        val2 = np.random.randint(1, 100, current_batch_size)

        keys = np.array(keys)
        val1 = np.array(val1)
        val2 = np.array(val2)
        val3 = np.array(val3)

        keys = keymap[keys - 1]

        keys.shape = (current_batch_size, 1)
        val1.shape = (current_batch_size, 1)
        val2.shape = (current_batch_size, 1)
        val3.shape = (current_batch_size, 1)

        rows = np.hstack((keys, val1, val2, val3))

        if write_thread is not None:
            write_thread.join()

        write_thread = threading.Thread(target=write_csv,
                                        args=(
                                            writer,
                                            rows,
                                        ))

        write_thread.start()

        #  for i in range(0, current_batch_size):
        #  buf.write(str(keys[i]))
        #  buf.write(delim)
        #  buf.write(str(val1[i]))
        #  buf.write(delim)
        #  buf.write(str(val2[i]))
        #  buf.write(delim)
        #  dummy_col = ''.join(
        #  random.choice(string.ascii_uppercase + string.digits)
        #  for _ in range(dummy_col_size))
        #  buf.write(dummy_col + "\n")
        #
        #  f.write(buf.getvalue())

        remaining -= current_batch_size

    if write_thread is not None:
        write_thread.join()

    return table_name, data_file
