import csv
import math
import os
import random
import string
import subprocess
import time
import threading

import numpy as np
import paramiko as pa
import scipy.stats as ss
import impala.dbapi as impaladb


def write_csv(writer, rows):
    writer.writerows(rows)

def load_data_to_hdfs(data_path):
    ssh = pa.SSHClient()
    ssh.load_system_host_keys()
    ssh.connect('cp-2')

    # create dir first




def create_table(host, port, schema_name, table_name):
    conn = impaladb.connect(host, port)
    cur = conn.cursor()
    cursor.execute("CREATE SCHEMA IF NOT EXISTS {0}".format(schema_name))
    cursor.fetchall()

    create_text_sql = """CREATE EXTERNAL TABLE IF NOT EXISTS `{0}`.`{1}` (
  `n_nationkey`  INT,
  `n_name`       STRING,
  `n_regionkey`  INT,
  `n_comment`    STRING,
  `n_dummy`      STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  LOCATION '/tmp/tpch100g/nation';
    """


def create_table_data(num_rows, num_keys, type, n, overwrite=False):
    dir = './raw_data'
    if not os.path.exists(dir):
        os.makedirs(dir)

    data_file = dir + '/' + "{0}n_{1}k_{2}_{3}.csv".format(
        num_rows, num_keys, type, n)

    if os.path.exists(data_file) and not overwrite:
        print("Data file: '{0}' already exists".format(data_file))
        return

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
