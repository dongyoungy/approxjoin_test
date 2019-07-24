import impala.dbapi as impaladb
#  import time
#  import string
#  import random
#  import multiprocessing
import hashlib
import uuid


def drop_result_table(conn, schema):
    cur = conn.cursor()
    drop_result_table_sql = """DROP TABLE IF EXISTS {}.results""".format(
        schema)
    cur.execute(drop_result_table_sql)
    cur.close()
    conn.commit()


def create_result_table(conn, schema):
    cur = conn.cursor()
    create_result_table_sql = """CREATE TABLE IF NOT EXISTS {}.results (query STRING, idx INT, left_dist STRING,
    right_dist STRING, p DOUBLE, q DOUBLE, actual DOUBLE, estimate DOUBLE, ts TIMESTAMP) STORED AS PARQUET
    """.format(schema)
    cur.execute(create_result_table_sql)
    conn.commit()
    cur.close()


def clean_temp_tables(host, port, schema):
    conn = impaladb.connect(host, port)
    cur = conn.cursor()
    cur.execute("SHOW TABLES IN {} LIKE 'result_temp_*'".format(schema))
    result = cur.fetchall()
    for row in result:
        table = row[0]
        sql = "DROP TABLE IF EXISTS {}.{}".format(schema, table)
        print(sql)
        conn.cursor().execute(sql)


def create_temp_table(conn, schema):
    random_str = hashlib.md5(str(uuid.uuid1().bytes).encode()).hexdigest()[:32]
    temp_table = "result_temp_" + random_str
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS {}.{}".format(schema, temp_table))
    conn.commit()
    cur.close()

    cur = conn.cursor()
    create_temp_table_sql = """CREATE TABLE IF NOT EXISTS {}.{} (query STRING, idx INT, left_dist STRING,
    right_dist STRING, p DOUBLE, q DOUBLE, actual DOUBLE, estimate DOUBLE, ts TIMESTAMP) STORED AS PARQUET
    """.format(schema, temp_table)
    cur.execute(create_temp_table_sql)
    conn.commit()
    cur.close()
    return temp_table


def run_synthetic_ours(host,
                       port,
                       table_schema,
                       sample_schema,
                       query_type,
                       left_dist,
                       right_dist,
                       num_sample,
                       overwrite=False):
    conn = impaladb.connect(host, port)
    if overwrite:
        drop_result_table(conn, sample_schema)
    # create result table
    create_result_table(conn, sample_schema)
    # create temp table
    temp_table = create_temp_table(conn, sample_schema)

    # get p,q,t1_join_col, t2_join_col, t1_agg_col
    sql = """SELECT p, q, t1_join_col, t2_join_col, t1_agg_col FROM {}.meta WHERE t1_name = '{}'
    AND t2_name = '{}' AND agg = '{}' ORDER BY ts DESC
    LIMIT 1""".format(sample_schema, left_dist, right_dist, query_type)
    cur = conn.cursor()
    cur.execute(sql)
    result = cur.fetchall()
    if cur.rowcount == 0:
        print("query empty!")
        return

    t1_join_col = ""
    t2_join_col = ""
    t1_agg_col = ""

    for row in result:
        p = row[0]
        q = row[1]
        t1_join_col = row[2]
        t2_join_col = row[3]
        t1_agg_col = row[4]

    # get actual value first
    cur = conn.cursor()
    if query_type == 'count':  # count
        actual_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
            table_schema, left_dist, right_dist)
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == 'sum':  # sum
        actual_sql = """SELECT SUM(t1.col1) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
            table_schema, left_dist, right_dist)
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == 'avg':  # avg
        actual_sql = """SELECT AVG(t1.col1) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
            table_schema, left_dist, right_dist)
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    else:
        print("Unsupported query: {}".format(query_type))
        return
    cur.close()

    # get start idx
    cur = conn.cursor()
    sql = """SELECT max(idx) FROM {}.results WHERE query = '{}' AND left_dist = '{}' AND
    right_dist = '{}' GROUP BY query, left_dist, right_dist""".format(
        sample_schema, query_type, left_dist, right_dist)
    cur.execute(sql)
    result = cur.fetchall()
    start_idx = 0
    for row in result:
        start_idx = row[0] if row[0] is not None else 0
    start_idx = start_idx + 1
    cur.close()

    # get estimate for each sample pair
    for i in range(start_idx, num_sample + 1):
        S1_name = "s__{}__{}__{}__{}__{}__{}__{}".format(left_dist, right_dist, t1_join_col,
                                                     t1_agg_col, query_type, i,
                                                     1)
        S2_name = "s__{}__{}__{}__{}__{}__{}".format(left_dist, right_dist, t2_join_col,
                                                 query_type, i, 2)

        cur = conn.cursor()
        if query_type == 'count':  # count
            estimate_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
                sample_schema, S1_name, S2_name)
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = estimate * (1 / (p * q * q))
        elif query_type == 'sum':  # sum
            estimate_sql = """SELECT SUM(t1.col1) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
                sample_schema, S1_name, S2_name)
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = estimate * (1 / (p * q * q))
        elif query_type == 'avg':  # avg
            estimate_sql = """SELECT COUNT(*) as val1, SUM(t1.col1) as val2 FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
                sample_schema, S1_name, S2_name)
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate_count = row[0] if row[0] is not None else 0
                estimate_sum = row[1] if row[1] is not None else 0
            estimate_sum = estimate_sum * (1 / (p * q * q))
            estimate_count = estimate_count * (1 / (p * q * q))
            estimate = estimate_sum / estimate_count if estimate_count != 0 else 0
        cur.close()

        # record result
        cur = conn.cursor()
        add_result_sql = """INSERT INTO TABLE {}.{} VALUES (
        '{}', {}, '{}', '{}', {:.3f}, {:.3f}, {}, {}, now()
        )""".format(sample_schema, temp_table, query_type, i, left_dist,
                    right_dist, p, q, actual, estimate)
        cur.execute(add_result_sql)
        cur.close()

    cur = conn.cursor()
    cur.execute("INSERT INTO TABLE {0}.results SELECT * FROM {0}.{1}".format(
        sample_schema, temp_table))
    conn.commit()
    cur.execute("DROP TABLE IF EXISTS {}.{}".format(sample_schema, temp_table))
    conn.commit()
    cur.close()


def run_synthetic_preset(host,
                         port,
                         table_schema,
                         sample_schema,
                         query_type,
                         left_dist,
                         right_dist,
                         p,
                         q,
                         num_sample,
                         overwrite=False):
    conn = impaladb.connect(host, port)
    if overwrite:
        drop_result_table(conn, sample_schema)

    # create result table
    create_result_table(conn, sample_schema)
    # create temp table
    temp_table = create_temp_table(conn, sample_schema)

    # get actual value first
    cur = conn.cursor()
    if query_type == 'count':  # count
        actual_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
            table_schema, left_dist, right_dist)
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == 'sum':  # sum
        actual_sql = """SELECT SUM(t1.col1) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
            table_schema, left_dist, right_dist)
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == 'avg':  # avg
        actual_sql = """SELECT AVG(t1.col1) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
            table_schema, left_dist, right_dist)
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    else:
        print("Unsupported query: {}".format(query_type))
        return
    cur.close()

    # get start idx
    cur = conn.cursor()
    sql = """SELECT max(idx) FROM {}.results WHERE query = '{}' AND left_dist = '{}' AND
    right_dist = '{}' and p = {} and q = {} GROUP BY query, left_dist, right_dist, p, q""".format(
        sample_schema, query_type, left_dist, right_dist, p, q)
    cur.execute(sql)
    result = cur.fetchall()
    start_idx = 0
    for row in result:
        start_idx = row[0] if row[0] is not None else 0
    start_idx = start_idx + 1
    cur.close()

    # get estimate for each sample pair
    for i in range(start_idx, num_sample + 1):
        S1_name = "s__{}__{}__{:.3f}p__{:.3f}q_{}_{}".format(
            left_dist, right_dist, p, q, i, 1)
        S2_name = "s__{}__{}__{:.3f}p__{:.3f}q_{}_{}".format(
            left_dist, right_dist, p, q, i, 2)
        S1_name = S1_name.replace('.', '_')
        S2_name = S2_name.replace('.', '_')
        cur = conn.cursor()
        if query_type == 'count':  # count
            estimate_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
                sample_schema, S1_name, S2_name)
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = estimate * (1 / (p * q * q))
        elif query_type == 'sum':  # sum
            estimate_sql = """SELECT SUM(t1.col1) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
                sample_schema, S1_name, S2_name)
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = estimate * (1 / (p * q * q))
        elif query_type == 'avg':  # avg
            estimate_sql = """SELECT COUNT(*) as val1, SUM(t1.col1) as val2 FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
                sample_schema, S1_name, S2_name)
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate_count = row[0] if row[0] is not None else 0
                estimate_sum = row[1] if row[1] is not None else 0
            estimate_sum = estimate_sum * (1 / (p * q * q))
            estimate_count = estimate_count * (1 / (p * q * q))
            estimate = estimate_sum / estimate_count if estimate_count != 0 else 0
        cur.close()

        # record result
        cur = conn.cursor()
        add_result_sql = """INSERT INTO TABLE {}.{} VALUES (
        '{}', {}, '{}', '{}', {:.3f}, {:.3f}, {}, {}, now()
        )""".format(sample_schema, temp_table, query_type, i, left_dist,
                    right_dist, p, q, actual, estimate)
        cur.execute(add_result_sql)
        cur.close()

    cur = conn.cursor()
    cur.execute("INSERT INTO TABLE {0}.results SELECT * FROM {0}.{1}".format(
        sample_schema, temp_table))
    conn.commit()
    cur.execute("DROP TABLE IF EXISTS {}.{}".format(sample_schema, temp_table))
    conn.commit()
    cur.close()


def run_instacart_ours(host,
                       port,
                       table_schema,
                       sample_schema,
                       query_type,
                       num_sample,
                       overwrite=False):
    conn = impaladb.connect(host, port)
    if overwrite:
        drop_result_table(conn, sample_schema)

    # create result table
    create_result_table(conn, sample_schema)
    # create temp table
    temp_table = create_temp_table(conn, sample_schema)

    # get p,q,t1_join_col, t2_join_col, t1_agg_col
    sql = """SELECT p, q, t1_join_col, t2_join_col, t1_agg_col FROM {}.meta WHERE
    agg = '{}' ORDER BY ts DESC
    LIMIT 1""".format(sample_schema, query_type)
    cur = conn.cursor()
    cur.execute(sql)
    result = cur.fetchall()
    for row in result:
        p = row[0]
        q = row[1]
        t1_join_col = row[2]
        t2_join_col = row[3]
        t1_agg_col = row[4]

    # get actual value first
    cur = conn.cursor()
    if query_type == 'count':  # count
        actual_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
        t1.order_id = t2.order_id
        WHERE t1.order_hour_of_day <> 1""".format(table_schema, 'orders',
                                                  'order_products')
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == 'sum':  # sum
        actual_sql = """SELECT SUM(t1.days_since_prior) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
        t1.order_id = t2.order_id
        WHERE t1.order_hour_of_day <> 1""".format(table_schema, 'orders',
                                                  'order_products')
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == 'avg':  # avg
        actual_sql = """SELECT AVG(t1.order_dow) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
        t1.order_id = t2.order_id
        WHERE t1.order_hour_of_day <> 1""".format(table_schema, 'orders',
                                                  'order_products')
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    else:
        print("Unsupported query: {}".format(query_type))
        return
    cur.close()

    # get start idx
    cur = conn.cursor()
    sql = """SELECT max(idx) FROM {}.results WHERE query = '{}' AND left_dist = '{}' AND
    right_dist = '{}' GROUP BY query, left_dist, right_dist""".format(
        sample_schema, query_type, 'orders', 'order_products')
    cur.execute(sql)
    result = cur.fetchall()
    start_idx = 0
    for row in result:
        start_idx = row[0] if row[0] is not None else 0
    start_idx = start_idx + 1
    cur.close()

    # get estimate for each sample pair
    for i in range(start_idx, num_sample + 1):
        S1_name = "s__{}__{}__{}__{}__{}__{}".format('orders', t1_join_col,
                                                     t1_agg_col, query_type, i,
                                                     1)
        S2_name = "s__{}__{}__{}__{}__{}".format('order_products', t2_join_col,
                                                 query_type, i, 2)

        cur = conn.cursor()
        if query_type == 'count':  # count
            estimate_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.order_id = t2.order_id
            WHERE t1.order_hour_of_day <> 1""".format(sample_schema, S1_name,
                                                      S2_name)
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = estimate * (1 / (p * q * q))
        elif query_type == 'sum':  # sum
            estimate_sql = """SELECT SUM(t1.days_since_prior) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.order_id = t2.order_id
            WHERE t1.order_hour_of_day <> 1""".format(sample_schema, S1_name,
                                                      S2_name)
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = float(estimate) * (1 / (p * q * q))
        elif query_type == 'avg':  # avg
            estimate_sql = """SELECT COUNT(*) as val1, SUM(t1.order_dow) as val2 FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.order_id = t2.order_id
            WHERE t1.order_hour_of_day <> 1""".format(sample_schema, S1_name,
                                                      S2_name)
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate_count = row[0] if row[0] is not None else 0
                estimate_sum = row[1] if row[1] is not None else 0
            estimate_sum = estimate_sum * (1 / (p * q * q))
            estimate_count = estimate_count * (1 / (p * q * q))
            estimate = estimate_sum / estimate_count if estimate_count != 0 else 0
        cur.close()

        # record result
        cur = conn.cursor()
        add_result_sql = """INSERT INTO TABLE {}.{} VALUES (
        '{}', {}, '{}', '{}', {:.3f}, {:.3f}, {}, {}, now()
        )""".format(sample_schema, temp_table, query_type, i, 'orders',
                    'order_products', p, q, actual, estimate)
        cur.execute(add_result_sql)
        cur.close()

    cur = conn.cursor()
    cur.execute("INSERT INTO TABLE {0}.results SELECT * FROM {0}.{1}".format(
        sample_schema, temp_table))
    conn.commit()
    cur.execute("DROP TABLE IF EXISTS {}.{}".format(sample_schema, temp_table))
    conn.commit()
    cur.close()


def run_instacart_preset(host,
                         port,
                         table_schema,
                         sample_schema,
                         query_type,
                         p,
                         q,
                         num_sample,
                         overwrite=False):
    conn = impaladb.connect(host, port)
    if overwrite:
        drop_result_table(conn, sample_schema)

    # create result table
    create_result_table(conn, sample_schema)
    # create temp table
    temp_table = create_temp_table(conn, sample_schema)

    # get actual value first
    cur = conn.cursor()
    if query_type == 'count':  # count
        actual_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
        t1.order_id = t2.order_id
        WHERE t1.order_hour_of_day <> 1""".format(table_schema, 'orders',
                                                  'order_products')
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == 'sum':  # sum
        actual_sql = """SELECT SUM(t1.days_since_prior) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
        t1.order_id = t2.order_id
        WHERE t1.order_hour_of_day <> 1""".format(table_schema, 'orders',
                                                  'order_products')
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == 'avg':  # avg
        actual_sql = """SELECT AVG(t1.order_dow) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
        t1.order_id = t2.order_id
        WHERE t1.order_hour_of_day <> 1""".format(table_schema, 'orders',
                                                  'order_products')
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    else:
        print("Unsupported query: {}".format(query_type))
        return
    cur.close()

    # get start idx
    cur = conn.cursor()
    sql = """SELECT max(idx) FROM {}.results WHERE query = '{}' AND left_dist = '{}' AND
    right_dist = '{}' and p = {} and q = {} GROUP BY query, left_dist, right_dist, p, q""".format(
        sample_schema, query_type, 'orders', 'order_products', p, q)
    cur.execute(sql)
    result = cur.fetchall()
    start_idx = 0
    for row in result:
        start_idx = row[0] if row[0] is not None else 0
    start_idx = start_idx + 1
    cur.close()

    # get estimate for each sample pair
    for i in range(start_idx, num_sample + 1):
        S1_name = "s__{}__{:.3f}p__{:.3f}q_{}_{}".format('orders', p, q, i, 1)
        S2_name = "s__{}__{:.3f}p__{:.3f}q_{}_{}".format(
            'order_products', p, q, i, 2)
        S1_name = S1_name.replace('.', '_')
        S2_name = S2_name.replace('.', '_')
        cur = conn.cursor()
        if query_type == 'count':  # count
            estimate_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.order_id = t2.order_id
            WHERE t1.order_hour_of_day <> 1""".format(sample_schema, S1_name,
                                                      S2_name)
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = estimate * (1 / (p * q * q))
        elif query_type == 'sum':  # sum
            estimate_sql = """SELECT SUM(t1.days_since_prior) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.order_id = t2.order_id
            WHERE t1.order_hour_of_day <> 1""".format(sample_schema, S1_name,
                                                      S2_name)
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = float(estimate) * (1 / (p * q * q))
        elif query_type == 'avg':  # avg
            estimate_sql = """SELECT COUNT(*) as val1, SUM(t1.order_dow) as val2 FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.order_id = t2.order_id
            WHERE t1.order_hour_of_day <> 1""".format(sample_schema, S1_name,
                                                      S2_name)
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate_count = row[0] if row[0] is not None else 0
                estimate_sum = row[1] if row[1] is not None else 0
            estimate_sum = estimate_sum * (1 / (p * q * q))
            estimate_count = estimate_count * (1 / (p * q * q))
            estimate = estimate_sum / estimate_count if estimate_count != 0 else 0
        cur.close()

        # record result
        cur = conn.cursor()
        add_result_sql = """INSERT INTO TABLE {}.{} VALUES (
        '{}', {}, '{}', '{}', {:.3f}, {:.3f}, {}, {}, now()
        )""".format(sample_schema, temp_table, query_type, i, 'orders',
                    'order_products', p, q, actual, estimate)
        cur.execute(add_result_sql)
        cur.close()

    cur = conn.cursor()
    cur.execute("INSERT INTO TABLE {0}.results SELECT * FROM {0}.{1}".format(
        sample_schema, temp_table))
    conn.commit()
    cur.execute("DROP TABLE IF EXISTS {}.{}".format(sample_schema, temp_table))
    conn.commit()
    cur.close()


def run_movielens_ours(host,
                       port,
                       table_schema,
                       sample_schema,
                       query_type,
                       num_sample,
                       overwrite=False):
    conn = impaladb.connect(host, port)
    if overwrite:
        drop_result_table(conn, sample_schema)

    # create result table
    create_result_table(conn, sample_schema)
    # create temp table
    temp_table = create_temp_table(conn, sample_schema)

    # get p,q,t1_join_col, t2_join_col, t1_agg_col
    sql = """SELECT p, q, t1_join_col, t2_join_col, t1_agg_col FROM {}.meta WHERE
    agg = '{}' ORDER BY ts DESC
    LIMIT 1""".format(sample_schema, query_type)
    cur = conn.cursor()
    cur.execute(sql)
    result = cur.fetchall()
    for row in result:
        p = row[0]
        q = row[1]
        t1_join_col = row[2]
        t2_join_col = row[3]
        t1_agg_col = row[4]

    # get actual value first
    cur = conn.cursor()
    if query_type == 'count':  # count
        actual_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
        t1.movieid = t2.movieid
        WHERE t2.genres NOT LIKE '%Documentary%'""".format(
            table_schema, 'ratings', 'movies')
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == 'sum':  # sum
        actual_sql = """SELECT SUM(rating) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
        t1.movieid = t2.movieid
        WHERE t2.genres NOT LIKE '%Documentary%'""".format(
            table_schema, 'ratings', 'movies')
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == 'avg':  # avg
        actual_sql = """SELECT AVG(rating) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
        t1.movieid = t2.movieid
        WHERE t2.genres NOT LIKE '%Documentary%'""".format(
            table_schema, 'ratings', 'movies')
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    else:
        print("Unsupported query: {}".format(query_type))
        return
    cur.close()

    # get start idx
    cur = conn.cursor()
    sql = """SELECT max(idx) FROM {}.results WHERE query = '{}' AND left_dist = '{}' AND
    right_dist = '{}' GROUP BY query, left_dist, right_dist""".format(
        sample_schema, query_type, 'ratings', 'movies')
    cur.execute(sql)
    result = cur.fetchall()
    start_idx = 0
    for row in result:
        start_idx = row[0] if row[0] is not None else 0
    start_idx = start_idx + 1
    cur.close()

    # get estimate for each sample pair
    for i in range(start_idx, num_sample + 1):
        S1_name = "s__{}__{}__{}__{}__{}__{}".format('ratings', t1_join_col,
                                                     t1_agg_col, query_type, i,
                                                     1)
        S2_name = "s__{}__{}__{}__{}__{}".format('movies', t2_join_col,
                                                 query_type, i, 2)
        cur = conn.cursor()
        if query_type == 'count':  # count
            estimate_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.movieid = t2.movieid
            WHERE t2.genres NOT LIKE '%Documentary%'""".format(
                sample_schema, S1_name, S2_name)
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = estimate * (1 / (p * q * q))
        elif query_type == 'sum':  # sum
            estimate_sql = """SELECT SUM(rating) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.movieid = t2.movieid
            WHERE t2.genres NOT LIKE '%Documentary%'""".format(
                sample_schema, S1_name, S2_name)
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = float(estimate) * (1 / (p * q * q))
        elif query_type == 'avg':  # avg
            estimate_sql = """SELECT COUNT(*) as val1, SUM(rating) as val2 FROM
            {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.movieid = t2.movieid
            WHERE t2.genres NOT LIKE '%Documentary%'""".format(
                sample_schema, S1_name, S2_name)
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate_count = row[0] if row[0] is not None else 0
                estimate_sum = row[1] if row[1] is not None else 0
            estimate_sum = float(estimate_sum) * (1 / (p * q * q))
            estimate_count = estimate_count * (1 / (p * q * q))
            estimate = estimate_sum / estimate_count if estimate_count != 0 else 0
        cur.close()

        # record result
        cur = conn.cursor()
        add_result_sql = """INSERT INTO TABLE {}.{} VALUES (
        '{}', {}, '{}', '{}', {:.3f}, {:.3f}, {}, {}, now()
        )""".format(sample_schema, temp_table, query_type, i, 'ratings',
                    'movies', p, q, actual, estimate)
        cur.execute(add_result_sql)
        cur.close()

    cur = conn.cursor()
    cur.execute("INSERT INTO TABLE {0}.results SELECT * FROM {0}.{1}".format(
        sample_schema, temp_table))
    conn.commit()
    cur.execute("DROP TABLE IF EXISTS {}.{}".format(sample_schema, temp_table))
    conn.commit()
    cur.close()


def run_movielens_preset(host,
                         port,
                         table_schema,
                         sample_schema,
                         query_type,
                         p,
                         q,
                         num_sample,
                         overwrite=False):
    conn = impaladb.connect(host, port)
    if overwrite:
        drop_result_table(conn, sample_schema)

    # create result table
    create_result_table(conn, sample_schema)
    # create temp table
    temp_table = create_temp_table(conn, sample_schema)

    # get actual value first
    cur = conn.cursor()
    if query_type == 'count':  # count
        actual_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
        t1.movieid = t2.movieid
        WHERE t2.genres NOT LIKE '%Documentary%'""".format(
            table_schema, 'ratings', 'movies')
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == 'sum':  # sum
        actual_sql = """SELECT SUM(rating) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
        t1.movieid = t2.movieid
        WHERE t2.genres NOT LIKE '%Documentary%'""".format(
            table_schema, 'ratings', 'movies')
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == 'avg':  # avg
        actual_sql = """SELECT AVG(rating) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
        t1.movieid = t2.movieid
        WHERE t2.genres NOT LIKE '%Documentary%'""".format(
            table_schema, 'ratings', 'movies')
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    else:
        print("Unsupported query: {}".format(query_type))
        return
    cur.close()

    # get start idx
    cur = conn.cursor()
    sql = """SELECT max(idx) FROM {}.results WHERE query = '{}' AND left_dist = '{}' AND
    right_dist = '{}' and p = {} and q = {} GROUP BY query, left_dist, right_dist, p, q""".format(
        sample_schema, query_type, 'ratings', 'movies', p, q)
    cur.execute(sql)
    result = cur.fetchall()
    start_idx = 0
    for row in result:
        start_idx = row[0] if row[0] is not None else 0
    start_idx = start_idx + 1
    cur.close()

    # get estimate for each sample pair
    for i in range(start_idx, num_sample + 1):
        S1_name = "s__{}__{:.3f}p__{:.3f}q_{}_{}".format('movies', p, q, i, 1)
        S2_name = "s__{}__{:.3f}p__{:.3f}q_{}_{}".format('ratings', p, q, i, 2)
        S1_name = S1_name.replace('.', '_')
        S2_name = S2_name.replace('.', '_')
        cur = conn.cursor()
        if query_type == 'count':  # count
            estimate_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.movieid = t2.movieid
            WHERE t1.genres NOT LIKE '%Documentary%'""".format(
                sample_schema, S1_name, S2_name)
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = estimate * (1 / (p * q * q))
        elif query_type == 'sum':  # sum
            estimate_sql = """SELECT SUM(rating) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.movieid = t2.movieid
            WHERE t1.genres NOT LIKE '%Documentary%'""".format(
                sample_schema, S1_name, S2_name)
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = float(estimate) * (1 / (p * q * q))
        elif query_type == 'avg':  # avg
            estimate_sql = """SELECT COUNT(*) as val1, SUM(rating) as val2 FROM
            {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.movieid = t2.movieid
            WHERE t1.genres NOT LIKE '%Documentary%'""".format(
                sample_schema, S1_name, S2_name)
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate_count = row[0] if row[0] is not None else 0
                estimate_sum = row[1] if row[1] is not None else 0
            estimate_sum = float(estimate_sum) * (1 / (p * q * q))
            estimate_count = estimate_count * (1 / (p * q * q))
            estimate = estimate_sum / estimate_count if estimate_count != 0 else 0
        cur.close()

        # record result
        cur = conn.cursor()
        add_result_sql = """INSERT INTO TABLE {}.{} VALUES (
        '{}', {}, '{}', '{}', {:.3f}, {:.3f}, {}, {}, now()
        )""".format(sample_schema, temp_table, query_type, i, 'ratings',
                    'movies', p, q, actual, estimate)
        cur.execute(add_result_sql)
        cur.close()

    cur = conn.cursor()
    cur.execute("INSERT INTO TABLE {0}.results SELECT * FROM {0}.{1}".format(
        sample_schema, temp_table))
    conn.commit()
    cur.execute("DROP TABLE IF EXISTS {}.{}".format(sample_schema, temp_table))
    conn.commit()
    cur.close()


def run_tpch_ours(host,
                  port,
                  table_schema,
                  sample_schema,
                  query_type,
                  num_sample,
                  overwrite=False):
    conn = impaladb.connect(host, port)
    if overwrite:
        drop_result_table(conn, sample_schema)

    # create result table
    create_result_table(conn, sample_schema)
    # create temp table
    temp_table = create_temp_table(conn, sample_schema)

    # get p,q,t1_join_col, t2_join_col, t1_agg_col
    sql = """SELECT p, q, t1_join_col, t2_join_col, t1_agg_col FROM {}.meta WHERE
    agg = '{}' ORDER BY ts DESC
    LIMIT 1""".format(sample_schema, query_type)
    cur = conn.cursor()
    cur.execute(sql)
    result = cur.fetchall()
    for row in result:
        p = row[0]
        q = row[1]
        t1_join_col = row[2]
        t2_join_col = row[3]
        t1_agg_col = row[4]

    # get actual value first
    cur = conn.cursor()
    if query_type == 'count':  # count
        actual_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
        t1.l_orderkey = t2.o_orderkey
        WHERE t2.o_orderstatus <> 'P'""".format(table_schema, 'lineitem',
                                                'orders')
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == 'sum':  # sum
        actual_sql = """SELECT SUM(l_quantity) FROM {0}.{1} t1
        JOIN {0}.{2} t2 ON
        t1.l_orderkey = t2.o_orderkey
        WHERE t2.o_orderstatus <> 'P'""".format(table_schema, 'lineitem',
                                                'orders')
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == 'avg':  # avg
        actual_sql = """SELECT AVG(l_extendedprice) FROM {0}.{1} t1
        JOIN {0}.{2} t2 ON
        t1.l_orderkey = t2.o_orderkey
        WHERE t2.o_orderstatus <> 'P'""".format(table_schema, 'lineitem',
                                                'orders')
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    else:
        print("Unsupported query: {}".format(query_type))
        return
    cur.close()

    # get start idx
    cur = conn.cursor()
    sql = """SELECT max(idx) FROM {}.results WHERE query = '{}' AND left_dist = '{}' AND
    right_dist = '{}' GROUP BY query, left_dist, right_dist""".format(
        sample_schema, query_type, 'lineitem', 'orders')
    cur.execute(sql)
    result = cur.fetchall()
    start_idx = 0
    for row in result:
        start_idx = row[0] if row[0] is not None else 0
    start_idx = start_idx + 1
    cur.close()

    # get estimate for each sample pair
    for i in range(start_idx, num_sample + 1):
        S1_name = "s__{}__{}__{}__{}__{}__{}".format('lineitem', t1_join_col,
                                                     t1_agg_col, query_type, i,
                                                     1)
        S2_name = "s__{}__{}__{}__{}__{}".format('orders', t2_join_col,
                                                 query_type, i, 2)
        cur = conn.cursor()
        if query_type == 'count':  # count
            estimate_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.l_orderkey = t2.o_orderkey
            WHERE t2.o_orderstatus <> 'P'""".format(sample_schema, S1_name,
                                                    S2_name)
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = estimate * (1 / (p * q * q))
        elif query_type == 'sum':  # sum
            estimate_sql = """SELECT SUM(l_quantity) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.l_orderkey = t2.o_orderkey
            WHERE t2.o_orderstatus <> 'P'""".format(sample_schema, S1_name,
                                                    S2_name)
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = float(estimate) * (1 / (p * q * q))
        elif query_type == 'avg':  # avg
            estimate_sql = """SELECT COUNT(*) as val1, SUM(l_extendedprice) as val2
            FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.l_orderkey = t2.o_orderkey
            WHERE t2.o_orderstatus <> 'P'""".format(sample_schema, S1_name,
                                                    S2_name)
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate_count = row[0] if row[0] is not None else 0
                estimate_sum = row[1] if row[1] is not None else 0
            estimate_sum = float(estimate_sum) * (1 / (p * q * q))
            estimate_count = estimate_count * (1 / (p * q * q))
            estimate = estimate_sum / estimate_count if estimate_count != 0 else 0
        cur.close()

        # record result
        cur = conn.cursor()
        add_result_sql = """INSERT INTO TABLE {}.{} VALUES (
        '{}', {}, '{}', '{}', {:.3f}, {:.3f}, {}, {}, now()
        )""".format(sample_schema, temp_table, query_type, i, 'lineitem',
                    'orders', p, q, actual, estimate)
        cur.execute(add_result_sql)
        cur.close()

    cur = conn.cursor()
    cur.execute("INSERT INTO TABLE {0}.results SELECT * FROM {0}.{1}".format(
        sample_schema, temp_table))
    conn.commit()
    cur.execute("DROP TABLE IF EXISTS {}.{}".format(sample_schema, temp_table))
    conn.commit()
    cur.close()


def run_tpch_preset(host,
                    port,
                    table_schema,
                    sample_schema,
                    query_type,
                    p,
                    q,
                    num_sample,
                    overwrite=False):
    conn = impaladb.connect(host, port)
    if overwrite:
        drop_result_table(conn, sample_schema)

    # create result table
    create_result_table(conn, sample_schema)
    # create temp table
    temp_table = create_temp_table(conn, sample_schema)

    # get actual value first
    cur = conn.cursor()
    if query_type == 'count':  # count
        actual_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
        t1.l_orderkey = t2.o_orderkey
        WHERE t2.o_orderstatus <> 'P'""".format(table_schema, 'lineitem',
                                                'orders')
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == 'sum':  # sum
        actual_sql = """SELECT SUM(l_quantity) FROM {0}.{1} t1
        JOIN {0}.{2} t2 ON
        t1.l_orderkey = t2.o_orderkey
        WHERE t2.o_orderstatus <> 'P'""".format(table_schema, 'lineitem',
                                                'orders')
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == 'avg':  # avg
        actual_sql = """SELECT AVG(l_extendedprice) FROM {0}.{1} t1
        JOIN {0}.{2} t2 ON
        t1.l_orderkey = t2.o_orderkey
        WHERE t2.o_orderstatus <> 'P'""".format(table_schema, 'lineitem',
                                                'orders')
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    else:
        print("Unsupported query: {}".format(query_type))
        return
    cur.close()

    # get start idx
    cur = conn.cursor()
    sql = """SELECT max(idx) FROM {}.results WHERE query = '{}' AND left_dist = '{}' AND
    right_dist = '{}' and p = {} and q = {} GROUP BY query, left_dist, right_dist, p, q""".format(
        sample_schema, query_type, 'lineitem', 'orders', p, q)
    cur.execute(sql)
    result = cur.fetchall()
    start_idx = 0
    for row in result:
        start_idx = row[0] if row[0] is not None else 0
    start_idx = start_idx + 1
    cur.close()

    # get estimate for each sample pair
    for i in range(start_idx, num_sample + 1):
        S1_name = "s__{}__{:.3f}p__{:.3f}q_{}_{}".format('orders', p, q, i, 1)
        S2_name = "s__{}__{:.3f}p__{:.3f}q_{}_{}".format(
            'lineitem', p, q, i, 2)
        S1_name = S1_name.replace('.', '_')
        S2_name = S2_name.replace('.', '_')
        cur = conn.cursor()
        if query_type == 'count':  # count
            estimate_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.o_orderkey = t2.l_orderkey
            WHERE t1.o_orderstatus <> 'P'""".format(sample_schema, S1_name,
                                                    S2_name)
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = estimate * (1 / (p * q * q))
        elif query_type == 'sum':  # sum
            estimate_sql = """SELECT SUM(l_quantity) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.o_orderkey = t2.l_orderkey
            WHERE t1.o_orderstatus <> 'P'""".format(sample_schema, S1_name,
                                                    S2_name)
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = float(estimate) * (1 / (p * q * q))
        elif query_type == 'avg':  # avg
            estimate_sql = """SELECT COUNT(*) as val1, SUM(l_extendedprice) as val2
            FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.o_orderkey = t2.l_orderkey
            WHERE t1.o_orderstatus <> 'P'""".format(sample_schema, S1_name,
                                                    S2_name)
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate_count = row[0] if row[0] is not None else 0
                estimate_sum = row[1] if row[1] is not None else 0
            estimate_sum = float(estimate_sum) * (1 / (p * q * q))
            estimate_count = estimate_count * (1 / (p * q * q))
            estimate = estimate_sum / estimate_count if estimate_count != 0 else 0
        cur.close()

        # record result
        cur = conn.cursor()
        add_result_sql = """INSERT INTO TABLE {}.{} VALUES (
        '{}', {}, '{}', '{}', {:.3f}, {:.3f}, {}, {}, now()
        )""".format(sample_schema, temp_table, query_type, i, 'lineitem',
                    'orders', p, q, actual, estimate)
        cur.execute(add_result_sql)
        cur.close()

    cur = conn.cursor()
    cur.execute("INSERT INTO TABLE {0}.results SELECT * FROM {0}.{1}".format(
        sample_schema, temp_table))
    conn.commit()
    cur.execute("DROP TABLE IF EXISTS {}.{}".format(sample_schema, temp_table))
    conn.commit()
    cur.close()
