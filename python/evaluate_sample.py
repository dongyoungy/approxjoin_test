import impala.dbapi as impaladb
import numpy as np

#  import time
#  import string
#  import random
#  import multiprocessing
import hashlib
import uuid


def drop_result_table(conn, schema):
    cur = conn.cursor()
    drop_result_table_sql = """DROP TABLE IF EXISTS {}.results""".format(schema)
    cur.execute(drop_result_table_sql)
    cur.close()
    conn.commit()


def drop_where_result_table(conn, schema):
    cur = conn.cursor()
    drop_result_table_sql = """DROP TABLE IF EXISTS {}.where_results""".format(schema)
    cur.execute(drop_result_table_sql)
    cur.close()
    conn.commit()


def create_result_table(conn, schema):
    cur = conn.cursor()
    create_result_table_sql = """CREATE TABLE IF NOT EXISTS {}.results (query STRING, idx INT, left_dist STRING,
    right_dist STRING, p DOUBLE, q DOUBLE, actual DOUBLE, estimate DOUBLE, ts TIMESTAMP) STORED AS PARQUET
    """.format(
        schema
    )
    cur.execute(create_result_table_sql)
    conn.commit()
    cur.close()


def create_strat_result_table(conn, schema):
    cur = conn.cursor()
    create_result_table_sql = """CREATE TABLE IF NOT EXISTS {}.strat_results
    (query STRING, idx INT, left_dist STRING,
    right_dist STRING, e1 DOUBLE, e2 DOUBLE, key_t BIGINT, row_t BIGINT, groupid INT,
    actual DOUBLE, estimate DOUBLE, ts TIMESTAMP) STORED AS PARQUET
    """.format(
        schema
    )
    cur.execute(create_result_table_sql)
    conn.commit()
    cur.close()


def create_where_result_table(conn, schema):
    cur = conn.cursor()
    create_result_table_sql = """CREATE TABLE IF NOT EXISTS {}.where_results
    (query STRING, idx INT, left_dist STRING,
    right_dist STRING, cond_type STRING, cond_val INT, where_dist STRING,
    p DOUBLE, q DOUBLE, actual DOUBLE, estimate DOUBLE, ts TIMESTAMP) STORED AS PARQUET
    """.format(
        schema
    )
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


#  def create_temp_table(conn, schema):
#  random_str = hashlib.md5(str(uuid.uuid1().bytes).encode()).hexdigest()[:32]
#  temp_table = "result_temp_" + random_str
#  cur = conn.cursor()
#  cur.execute("DROP TABLE IF EXISTS {}.{}".format(schema, temp_table))
#  conn.commit()
#  cur.close()
#
#  cur = conn.cursor()
#  create_temp_table_sql = """CREATE TABLE IF NOT EXISTS {}.{} (query STRING, idx INT, left_dist STRING,
#  right_dist STRING, p DOUBLE, q DOUBLE, actual DOUBLE, estimate DOUBLE, ts TIMESTAMP) STORED AS PARQUET
#  """.format(schema, temp_table)
#  cur.execute(create_temp_table_sql)
#  conn.commit()
#  cur.close()
#  return temp_table


def create_temp_table(conn, schema, orig_table=None):
    random_str = hashlib.md5(str(uuid.uuid1().bytes).encode()).hexdigest()[:32]
    temp_table = "result_temp_" + random_str
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS {}.{}".format(schema, temp_table))
    conn.commit()
    cur.close()

    cur = conn.cursor()
    if orig_table is not None:
        create_temp_table_sql = """CREATE TABLE IF NOT EXISTS {0}.{1} LIKE {0}.{2} STORED AS PARQUET
        """.format(
            schema, temp_table, orig_table
        )
    else:
        create_temp_table_sql = """CREATE TABLE IF NOT EXISTS {}.{} (query STRING, idx INT, left_dist STRING,
        right_dist STRING, p DOUBLE, q DOUBLE, actual DOUBLE, estimate DOUBLE, ts TIMESTAMP) STORED AS PARQUET
        """.format(
            schema, temp_table
        )
    cur.execute(create_temp_table_sql)
    conn.commit()
    cur.close()
    return temp_table


def test_synthetic_ours(
    host,
    port,
    table_schema,
    sample_schema,
    query_type,
    left_dist,
    right_dist,
    num_sample,
    overwrite=False,
):
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
    LIMIT 1""".format(
        sample_schema, left_dist, right_dist, query_type
    )
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
    if query_type == "count":  # count
        actual_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
            table_schema, left_dist, right_dist
        )
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == "sum":  # sum
        actual_sql = """SELECT SUM(t1.col2) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
            table_schema, left_dist, right_dist
        )
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == "avg":  # avg
        actual_sql = """SELECT AVG(t1.col2) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
            table_schema, left_dist, right_dist
        )
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    else:
        print("Unsupported query: {}".format(query_type))
        return
    cur.close()

    # get estimate for each sample pair
    for i in range(1, num_sample + 1):
        S1_name = "s__{}__{}__{}__{}__{}__{}__{}".format(
            left_dist, right_dist, t1_join_col, t1_agg_col, query_type, i, 1
        )
        S2_name = "s__{}__{}__{}__{}__{}__{}".format(
            left_dist, right_dist, t2_join_col, query_type, i, 2
        )

        cur = conn.cursor()
        if query_type == "count":  # count
            estimate_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
                sample_schema, S1_name, S2_name
            )
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = estimate * (1 / (p * q * q))
        elif query_type == "sum":  # sum
            estimate_sql = """SELECT SUM(t1.col2) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
                sample_schema, S1_name, S2_name
            )
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = estimate * (1 / (p * q * q))
        elif query_type == "avg":  # avg
            estimate_sql = """SELECT COUNT(*) as val1, SUM(t1.col2) as val2 FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
                sample_schema, S1_name, S2_name
            )
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate_count = row[0] if row[0] is not None else 0
                estimate_sum = row[1] if row[1] is not None else 0
            estimate_sum = estimate_sum * (1 / (p * q * q))
            estimate_count = estimate_count * (1 / (p * q * q))
            estimate = estimate_sum / estimate_count if estimate_count != 0 else 0
        cur.close()
        print((i, actual, estimate))


def run_synthetic_ours(
    host,
    port,
    table_schema,
    sample_schema,
    query_type,
    left_dist,
    right_dist,
    num_sample,
    overwrite=False,
):
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
    LIMIT 1""".format(
        sample_schema, left_dist, right_dist, query_type
    )
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

    print((p, q))

    # get actual value first
    cur = conn.cursor()
    if query_type == "count":  # count
        actual_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
            table_schema, left_dist, right_dist
        )
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == "sum":  # sum
        actual_sql = """SELECT SUM(t1.col2) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
            table_schema, left_dist, right_dist
        )
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == "avg":  # avg
        actual_sql = """SELECT AVG(t1.col2) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
            table_schema, left_dist, right_dist
        )
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
        sample_schema, query_type, left_dist, right_dist
    )
    cur.execute(sql)
    result = cur.fetchall()
    start_idx = 0
    for row in result:
        start_idx = row[0] if row[0] is not None else 0
    start_idx = start_idx + 1
    cur.close()

    # get estimate for each sample pair
    for i in range(start_idx, num_sample + 1):
        S1_name = "s__{}__{}__{}__{}__{}__{}__{}".format(
            left_dist, right_dist, t1_join_col, t1_agg_col, query_type, i, 1
        )
        S2_name = "s__{}__{}__{}__{}__{}__{}".format(
            left_dist, right_dist, t2_join_col, query_type, i, 2
        )

        cur = conn.cursor()
        if query_type == "count":  # count
            estimate_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
                sample_schema, S1_name, S2_name
            )
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = estimate * (1 / (p * q * q))
        elif query_type == "sum":  # sum
            estimate_sql = """SELECT SUM(t1.col2) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
                sample_schema, S1_name, S2_name
            )
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = estimate * (1 / (p * q * q))
        elif query_type == "avg":  # avg
            estimate_sql = """SELECT COUNT(*) as val1, SUM(t1.col2) as val2 FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
                sample_schema, S1_name, S2_name
            )
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
        )""".format(
            sample_schema,
            temp_table,
            query_type,
            i,
            left_dist,
            right_dist,
            p,
            q,
            actual,
            estimate,
        )
        cur.execute(add_result_sql)
        cur.close()

    cur = conn.cursor()
    cur.execute(
        "INSERT INTO TABLE {0}.results SELECT * FROM {0}.{1}".format(
            sample_schema, temp_table
        )
    )
    conn.commit()
    cur.execute("DROP TABLE IF EXISTS {}.{}".format(sample_schema, temp_table))
    conn.commit()
    cur.close()


def run_synthetic_ours_with_all_sample(
    host,
    port,
    table_schema,
    sample_schema,
    query_type,
    left_dist,
    right_dist,
    num_sample,
    overwrite=False,
):
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
    LIMIT 1""".format(
        sample_schema, left_dist, right_dist, "all"
    )
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
    if query_type == "count":  # count
        actual_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
            table_schema, left_dist, right_dist
        )
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == "sum":  # sum
        actual_sql = """SELECT SUM(t1.col2) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
            table_schema, left_dist, right_dist
        )
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == "avg":  # avg
        actual_sql = """SELECT AVG(t1.col2) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
            table_schema, left_dist, right_dist
        )
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
        sample_schema, query_type, left_dist, right_dist
    )
    cur.execute(sql)
    result = cur.fetchall()
    start_idx = 0
    for row in result:
        start_idx = row[0] if row[0] is not None else 0
    start_idx = start_idx + 1
    cur.close()

    # get estimate for each sample pair
    for i in range(start_idx, num_sample + 1):
        S1_name = "s__{}__{}__{}__{}__{}__{}".format(
            left_dist, right_dist, t1_join_col, "all", i, 1
        )
        S2_name = "s__{}__{}__{}__{}__{}__{}".format(
            left_dist, right_dist, t2_join_col, "all", i, 2
        )

        cur = conn.cursor()
        if query_type == "count":  # count
            estimate_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
                sample_schema, S1_name, S2_name
            )
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = estimate * (1 / (p * q * q))
        elif query_type == "sum":  # sum
            estimate_sql = """SELECT SUM(t1.col2) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
                sample_schema, S1_name, S2_name
            )
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = estimate * (1 / (p * q * q))
        elif query_type == "avg":  # avg
            estimate_sql = """SELECT COUNT(*) as val1, SUM(t1.col2) as val2 FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
                sample_schema, S1_name, S2_name
            )
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
        )""".format(
            sample_schema,
            temp_table,
            query_type,
            i,
            left_dist,
            right_dist,
            p,
            q,
            actual,
            estimate,
        )
        cur.execute(add_result_sql)
        cur.close()

    cur = conn.cursor()
    cur.execute(
        "INSERT INTO TABLE {0}.results SELECT * FROM {0}.{1}".format(
            sample_schema, temp_table
        )
    )
    conn.commit()
    cur.execute("DROP TABLE IF EXISTS {}.{}".format(sample_schema, temp_table))
    conn.commit()
    cur.close()


def run_synthetic_ours_with_where(
    host,
    port,
    table_schema,
    sample_schema,
    query_type,
    left_dist,
    right_dist,
    cond_type,
    where_dist,
    num_sample,
    overwrite=False,
):
    conn = impaladb.connect(host, port)
    if overwrite:
        drop_where_result_table(conn, sample_schema)
    # create result table
    create_where_result_table(conn, sample_schema)
    # create temp table
    temp_table = create_temp_table(conn, sample_schema, "where_results")
    result_table = "where_results"

    # get p,q,t1_join_col, t2_join_col, t1_agg_col
    sql = """SELECT p, q, t1_join_col, t2_join_col, t1_agg_col, t1_where_col FROM {}.meta WHERE t1_name = '{}'
    AND t2_name = '{}' AND agg = '{}' and cond_type = '{}' and where_dist = '{}' ORDER BY ts DESC
    LIMIT 1""".format(
        sample_schema, left_dist, right_dist, query_type, cond_type, where_dist
    )
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

    if cond_type == "eq":
        cond_op = "="
        increment = 5
    elif cond_type == "geq":
        cond_op = ">="
        increment = 10
    else:
        print("Unsupported Cond Op: {}".format(cond_type))
        return
    t1_where_col = "t1.col3"

    cur = conn.cursor()
    cur.execute(
        "SELECT MIN({0}), MAX({0}) FROM {1}.{2} t1".format(
            t1_where_col, table_schema, left_dist
        )
    )
    row = cur.fetchone()
    start_val = row[0]
    end_val = row[1]

    for cond_val in range(start_val, end_val + 1, increment):
        where_str = "WHERE {} {} {}".format(t1_where_col, cond_op, cond_val)
        # get actual value first
        cur = conn.cursor()
        if query_type == "count":  # count
            actual_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1 {3}""".format(
                table_schema, left_dist, right_dist, where_str
            )
            cur.execute(actual_sql)
            result = cur.fetchall()
            for row in result:
                actual = row[0] if row[0] is not None else 0
        elif query_type == "sum":  # sum
            actual_sql = """SELECT SUM(t1.col2) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1 {3}""".format(
                table_schema, left_dist, right_dist, where_str
            )
            cur.execute(actual_sql)
            result = cur.fetchall()
            for row in result:
                actual = row[0] if row[0] is not None else 0
        elif query_type == "avg":  # avg
            actual_sql = """SELECT AVG(t1.col2) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1 {3}""".format(
                table_schema, left_dist, right_dist, where_str
            )
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
        sql = """SELECT max(idx) FROM {}.{} WHERE query = '{}' AND left_dist = '{}' AND
        right_dist = '{}' AND cond_type = '{}' AND cond_val = {} AND where_dist = '{}'
        GROUP BY query, left_dist, right_dist, cond_type, cond_val, where_dist""".format(
            sample_schema,
            result_table,
            query_type,
            left_dist,
            right_dist,
            cond_type,
            cond_val,
            where_dist,
        )
        cur.execute(sql)
        result = cur.fetchall()
        start_idx = 0
        for row in result:
            start_idx = row[0] if row[0] is not None else 0
        start_idx = start_idx + 1
        cur.close()

        # get estimate for each sample pair
        for i in range(start_idx, num_sample + 1):
            S1_name = "s__{}__{}__{}__{}__{}__{}__{}__{}__{}".format(
                left_dist,
                right_dist,
                t1_join_col,
                t1_agg_col,
                query_type,
                cond_type,
                where_dist,
                i,
                1,
            )
            S2_name = "s__{}__{}__{}__{}__{}__{}__{}__{}".format(
                left_dist,
                right_dist,
                t2_join_col,
                query_type,
                cond_type,
                where_dist,
                i,
                2,
            )

            cur = conn.cursor()
            if query_type == "count":  # count
                estimate_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1 {3}""".format(
                    sample_schema, S1_name, S2_name, where_str
                )
                cur.execute(estimate_sql)
                result = cur.fetchall()
                for row in result:
                    estimate = row[0] if row[0] is not None else 0
                estimate = estimate * (1 / (p * q * q))
            elif query_type == "sum":  # sum
                estimate_sql = """SELECT SUM(t1.col2) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1 {3}""".format(
                    sample_schema, S1_name, S2_name, where_str
                )
                cur.execute(estimate_sql)
                result = cur.fetchall()
                for row in result:
                    estimate = row[0] if row[0] is not None else 0
                estimate = estimate * (1 / (p * q * q))
            elif query_type == "avg":  # avg
                estimate_sql = """SELECT COUNT(*) as val1, SUM(t1.col2) as val2 FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1 {3}""".format(
                    sample_schema, S1_name, S2_name, where_str
                )
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
            '{}', {}, '{}', '{}', '{}', {}, '{}', {:.3f}, {:.3f}, {}, {}, now()
            )""".format(
                sample_schema,
                temp_table,
                query_type,
                i,
                left_dist,
                right_dist,
                cond_type,
                cond_val,
                where_dist,
                p,
                q,
                actual,
                estimate,
            )
            cur.execute(add_result_sql)
            cur.close()

    cur = conn.cursor()
    cur.execute(
        "INSERT INTO TABLE {0}.{2} SELECT * FROM {0}.{1}".format(
            sample_schema, temp_table, result_table
        )
    )
    conn.commit()
    cur.execute("DROP TABLE IF EXISTS {}.{}".format(sample_schema, temp_table))
    conn.commit()
    cur.close()


def run_synthetic_preset_with_where(
    host,
    port,
    table_schema,
    sample_schema,
    query_type,
    left_dist,
    right_dist,
    cond_type,
    p,
    q,
    num_sample,
    overwrite=False,
):
    conn = impaladb.connect(host, port)
    if overwrite:
        drop_where_result_table(conn, sample_schema)

    # create result table
    create_where_result_table(conn, sample_schema)
    # create temp table
    temp_table = create_temp_table(conn, sample_schema, "where_results")
    result_table = "where_results"

    if cond_type == "eq":
        increment = 5
        cond_op = "="
    elif cond_type == "geq":
        increment = 10
        cond_op = ">="
    else:
        print("Unsupported Cond Op: {}".format(cond_type))
        return
    t1_where_col = "t1.col3"

    cur = conn.cursor()
    cur.execute(
        "SELECT MIN({0}), MAX({0}) FROM {1}.{2} t1".format(
            t1_where_col, table_schema, left_dist
        )
    )
    row = cur.fetchone()
    start_val = row[0]
    end_val = row[1]

    for cond_val in range(start_val, end_val + 1, increment):
        where_str = "WHERE {} {} {}".format(t1_where_col, cond_op, cond_val)

        # get actual value first
        cur = conn.cursor()
        if query_type == "count":  # count
            actual_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1 {3}""".format(
                table_schema, left_dist, right_dist, where_str
            )
            cur.execute(actual_sql)
            result = cur.fetchall()
            for row in result:
                actual = row[0] if row[0] is not None else 0
        elif query_type == "sum":  # sum
            actual_sql = """SELECT SUM(t1.col2) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1 {3}""".format(
                table_schema, left_dist, right_dist, where_str
            )
            cur.execute(actual_sql)
            result = cur.fetchall()
            for row in result:
                actual = row[0] if row[0] is not None else 0
        elif query_type == "avg":  # avg
            actual_sql = """SELECT AVG(t1.col2) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1 {3}""".format(
                table_schema, left_dist, right_dist, where_str
            )
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
        sql = """SELECT max(idx) FROM {}.{} WHERE query = '{}' AND left_dist = '{}' AND
        right_dist = '{}' and p = {} and q = {} and
        cond_type = '{}' AND cond_val = {}
        GROUP BY query, left_dist, right_dist, p, q, cond_type, cond_val""".format(
            sample_schema,
            result_table,
            query_type,
            left_dist,
            right_dist,
            p,
            q,
            cond_type,
            cond_val,
        )
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
                left_dist, right_dist, p, q, i, 1
            )
            S2_name = "s__{}__{}__{:.3f}p__{:.3f}q_{}_{}".format(
                left_dist, right_dist, p, q, i, 2
            )
            S1_name = S1_name.replace(".", "_")
            S2_name = S2_name.replace(".", "_")
            cur = conn.cursor()
            if query_type == "count":  # count
                estimate_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1 {3}""".format(
                    sample_schema, S1_name, S2_name, where_str
                )
                cur.execute(estimate_sql)
                result = cur.fetchall()
                for row in result:
                    estimate = row[0] if row[0] is not None else 0
                estimate = estimate * (1 / (p * q * q))
            elif query_type == "sum":  # sum
                estimate_sql = """SELECT SUM(t1.col2) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1 {3}""".format(
                    sample_schema, S1_name, S2_name, where_str
                )
                cur.execute(estimate_sql)
                result = cur.fetchall()
                for row in result:
                    estimate = row[0] if row[0] is not None else 0
                estimate = estimate * (1 / (p * q * q))
            elif query_type == "avg":  # avg
                estimate_sql = """SELECT COUNT(*) as val1, SUM(t1.col2) as val2 FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1 {3}""".format(
                    sample_schema, S1_name, S2_name, where_str
                )
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
            '{}', {}, '{}', '{}', '{}', {}, '{}', {:.3f}, {:.3f}, {}, {}, now()
            )""".format(
                sample_schema,
                temp_table,
                query_type,
                i,
                left_dist,
                right_dist,
                cond_type,
                cond_val,
                "N/A",
                p,
                q,
                actual,
                estimate,
            )
            cur.execute(add_result_sql)
            cur.close()

    cur = conn.cursor()
    cur.execute(
        "INSERT INTO TABLE {0}.{2} SELECT * FROM {0}.{1}".format(
            sample_schema, temp_table, result_table
        )
    )
    conn.commit()
    cur.execute("DROP TABLE IF EXISTS {}.{}".format(sample_schema, temp_table))
    conn.commit()
    cur.close()


def run_synthetic_preset(
    host,
    port,
    table_schema,
    sample_schema,
    query_type,
    left_dist,
    right_dist,
    p,
    q,
    num_sample,
    overwrite=False,
):
    conn = impaladb.connect(host, port)
    if overwrite:
        drop_result_table(conn, sample_schema)

    # create result table
    create_result_table(conn, sample_schema)
    # create temp table
    temp_table = create_temp_table(conn, sample_schema)

    # get actual value first
    cur = conn.cursor()
    if query_type == "count":  # count
        actual_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
            table_schema, left_dist, right_dist
        )
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == "sum":  # sum
        actual_sql = """SELECT SUM(t1.col2) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
            table_schema, left_dist, right_dist
        )
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == "avg":  # avg
        actual_sql = """SELECT AVG(t1.col2) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
            table_schema, left_dist, right_dist
        )
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
        sample_schema, query_type, left_dist, right_dist, p, q
    )
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
            left_dist, right_dist, p, q, i, 1
        )
        S2_name = "s__{}__{}__{:.3f}p__{:.3f}q_{}_{}".format(
            left_dist, right_dist, p, q, i, 2
        )
        S1_name = S1_name.replace(".", "_")
        S2_name = S2_name.replace(".", "_")
        cur = conn.cursor()
        if query_type == "count":  # count
            estimate_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
                sample_schema, S1_name, S2_name
            )
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = estimate * (1 / (p * q * q))
        elif query_type == "sum":  # sum
            estimate_sql = """SELECT SUM(t1.col2) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
                sample_schema, S1_name, S2_name
            )
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = estimate * (1 / (p * q * q))
        elif query_type == "avg":  # avg
            estimate_sql = """SELECT COUNT(*) as val1, SUM(t1.col2) as val2 FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1""".format(
                sample_schema, S1_name, S2_name
            )
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
        )""".format(
            sample_schema,
            temp_table,
            query_type,
            i,
            left_dist,
            right_dist,
            p,
            q,
            actual,
            estimate,
        )
        cur.execute(add_result_sql)
        cur.close()

    cur = conn.cursor()
    cur.execute(
        "INSERT INTO TABLE {0}.results SELECT * FROM {0}.{1}".format(
            sample_schema, temp_table
        )
    )
    conn.commit()
    cur.execute("DROP TABLE IF EXISTS {}.{}".format(sample_schema, temp_table))
    conn.commit()
    cur.close()


def run_synthetic_stratified_ours(
    host,
    port,
    table_schema,
    sample_schema,
    left_dist,
    right_dist,
    query_type,
    e1, e2,
    key_t,
    row_t,
    num_sample,
    overwrite=False,
):
    conn = impaladb.connect(host, port)
    if overwrite:
        drop_result_table(conn, sample_schema)

    # create result table
    create_strat_result_table(conn, sample_schema)
    # create temp table
    temp_table = create_temp_table(conn, sample_schema, 'strat_results')

    cur = conn.cursor()
    sql = """
    SELECT count(distinct col3) FROM {0}.{1}
    """.format(table_schema, left_dist)
    cur.execute(sql)
    res = cur.fetchone()

    num_grp = res[0]

    actual = np.zeros((res[0]))
    group_info = [None] * num_grp

    # get actual value first
    if query_type == "count":  # count
        actual_sql = """SELECT t1.col3, COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1 GROUP BY t1.col3""".format(
            table_schema, left_dist, right_dist
        )
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual[row[0]] = row[1] 
    elif query_type == "sum":  # sum
        actual_sql = """SELECT t1.col3, SUM(t1.col2) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1 GROUP BY t1.col3""".format(
            table_schema, left_dist, right_dist
        )
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual[row[0]] = row[1] 
    elif query_type == "avg":  # avg
        actual_sql = """SELECT t1.col3, AVG(t1.col2) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1 GROUP BY t1.col3""".format(
            table_schema, left_dist, right_dist
        )
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual[row[0]] = row[1] 
    else:
        print("Unsupported query: {}".format(query_type))
        return
    cur.close()

    # get start idx
    cur = conn.cursor()
    sql = """SELECT max(idx) FROM {}.strat_results WHERE query = '{}' AND left_dist = '{}' AND
    right_dist = '{}' and query = '{}' and e1 = {} and e2 = {} and key_t = {} and row_t = {}
    GROUP BY query, left_dist, right_dist""".format(
        sample_schema, query_type, left_dist, right_dist, query_type, e1, e2, key_t, row_t
    )
    cur.execute(sql)
    result = cur.fetchall()
    start_idx = 0
    for row in result:
        start_idx = row[0] if row[0] is not None else 0
    start_idx = start_idx + 1
    cur.close()

    cur = conn.cursor()
    group_info_table = "{}__{}__{}__groupinfo".format(left_dist, right_dist, query_type)
    cur.execute("""
    SELECT DISTINCT groupid, is_major, p1, q1, p2_major, q2_major, p2_minor, q2_minor 
    FROM {}.{}
    WHERE row_t = {} AND key_t = {}
    """.format(sample_schema, group_info_table, row_t, key_t))
    result = cur.fetchall()
    for row in result:
        group_info[row[0]] = (row[1], row[2], row[3], row[4], row[5], row[6], row[7])

    # get estimate for each sample pair
    for i in range(start_idx, num_sample + 1):
        estimates = np.zeros((num_grp))
        S1_name = "s__{}__{}__{}__{}__{}__{}__{}__{}__{:.3f}__{}__{}".format(
            left_dist,
            right_dist,
            'col1',
            'col2',
            'col3',
            query_type,
            key_t,
            row_t,
            e1,
            i,
            1,
        )
        S2_major_name = "s__{}__{}__{}__{}__{}__{}__{:.3f}__{}__{}_major".format(
            left_dist, right_dist, 'col1', query_type, key_t, row_t, e2, i, 2
        )
        S2_minor_name = "s__{}__{}__{}__{}__{}__{}__{:.3f}__{}__{}_minor".format(
            left_dist, right_dist, 'col1', query_type, key_t, row_t, e2, i, 2
        )
        S1_name = S1_name.replace(".", "_")
        S2_major_name = S2_major_name.replace(".", "_")
        S2_minor_name = S2_minor_name.replace(".", "_")
        cur = conn.cursor()
        if query_type == "count":  # count
            estimate_sql = """
            select * from
            (
            select grp, c * (1 / (p1 *  q1 * q2_major)) as estimate from
            (select s1.col3 as grp, count(*) as c, sum(s1.col2) as s from
            {0}.{1} s1 join 
            {0}.{2} s2m on s1.col1 = s2m.col1
            group by s1.col3) t1 join {0}.{4} ginfo on t1.grp = ginfo.groupid
            where ginfo.is_major = 1 and ginfo.key_t = {5} and ginfo.row_t = {6}
            union
            select grp, c * (1 / (p1 *  (case when q1 > 1 then 1 else q1 end) * q2_minor)) as estimate from
            (select s1.col3 as grp, count(*) as c, sum(s1.col2) as s from
            {0}.{1} s1 join 
            {0}.{3} s2m on s1.col1 = s2m.col1
            group by s1.col3) t1 join {0}.{4} ginfo on t1.grp = ginfo.groupid
            where ginfo.is_major = 0 and ginfo.key_t = {5} and ginfo.row_t = {6}
            ) tmp
            order by grp;
            """.format(
                sample_schema, S1_name, S2_major_name, S2_minor_name, group_info_table, key_t, row_t
            )
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                grp = row[0]
                estimate = row[1] if row[1] is not None else 0
                estimates[row[0]] = estimate

        elif query_type == "sum":  # sum
            estimate_sql = """
            select * from
            (
            select grp, s * (1 / (p1 *  q1 * q2_major)) as estimate from
            (select s1.col3 as grp, count(*) as c, sum(s1.col2) as s from
            {0}.{1} s1 join 
            {0}.{2} s2m on s1.col1 = s2m.col1
            group by s1.col3) t1 join {0}.{4} ginfo on t1.grp = ginfo.groupid
            where ginfo.is_major = 1 and ginfo.key_t = {5} and ginfo.row_t = {6}
            union
            select grp, s * (1 / (p1 *  (case when q1 > 1 then 1 else q1 end) * q2_minor)) as estimate from
            (select s1.col3 as grp, count(*) as c, sum(s1.col2) as s from
            {0}.{1} s1 join 
            {0}.{3} s2m on s1.col1 = s2m.col1
            group by s1.col3) t1 join {0}.{4} ginfo on t1.grp = ginfo.groupid
            where ginfo.is_major = 0 and ginfo.key_t = {5} and ginfo.row_t = {6}
            ) tmp
            order by grp;
            """.format(
                sample_schema, S1_name, S2_major_name, S2_minor_name, group_info_table, key_t, row_t
            )
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                grp = row[0]
                estimate = row[1] if row[1] is not None else 0
                estimates[row[0]] = estimate
        elif query_type == "avg":  # avg
            estimate_sql = """
            select * from
            (
            select grp, s/c as estimate from
            (select s1.col3 as grp, count(*) as c, sum(s1.col2) as s from
            {0}.{1} s1 join 
            {0}.{2} s2m on s1.col1 = s2m.col1
            group by s1.col3) t1 join {0}.{4} ginfo on t1.grp = ginfo.groupid
            where ginfo.is_major = 1 and ginfo.key_t = {5} and ginfo.row_t = {6}
            union
            select grp, s/c as estimate from
            (select s1.col3 as grp, count(*) as c, sum(s1.col2) as s from
            {0}.{1} s1 join 
            {0}.{3} s2m on s1.col1 = s2m.col1
            group by s1.col3) t1 join {0}.{4} ginfo on t1.grp = ginfo.groupid
            where ginfo.is_major = 0 and ginfo.key_t = {5} and ginfo.row_t = {6}
            ) tmp
            order by grp;
            """.format(
                sample_schema, S1_name, S2_major_name, S2_minor_name, group_info_table, key_t, row_t
            )
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                grp = row[0]
                estimate = row[1] if row[1] is not None else 0
                estimates[row[0]] = estimate
        cur.close()

        # record result
        cur = conn.cursor()
        for grp in range(0, num_grp):
            if estimates[grp] != 0:
                add_result_sql = """INSERT INTO TABLE {}.{} VALUES (
                '{}', {}, '{}', '{}', {:.3f}, {:.3f}, {}, {}, {}, {}, {}, now()
                )""".format(
                    sample_schema,
                    temp_table,
                    query_type,
                    i,
                    left_dist,
                    right_dist,
                    e1, e2, key_t, row_t,
                    grp,
                    actual[grp],
                    estimates[grp],
                )
                cur.execute(add_result_sql)
        cur.close()

    cur = conn.cursor()
    cur.execute(
        "INSERT INTO TABLE {0}.strat_results SELECT * FROM {0}.{1}".format(
            sample_schema, temp_table
        )
    )
    conn.commit()
    cur.execute("DROP TABLE IF EXISTS {}.{}".format(sample_schema, temp_table))
    conn.commit()
    cur.close()

def run_synthetic_stratified_preset(
    host,
    port,
    table_schema,
    sample_schema,
    left_dist,
    right_dist,
    query_type,
    K,
    q,
    num_sample,
    overwrite=False,
):
    conn = impaladb.connect(host, port)
    if overwrite:
        drop_result_table(conn, sample_schema)

    # create result table
    create_strat_result_table(conn, sample_schema)
    # create temp table
    temp_table = create_temp_table(conn, sample_schema, 'strat_results')

    actual = np.zeros((10))

    # get actual value first
    cur = conn.cursor()
    if query_type == "count":  # count
        actual_sql = """SELECT t1.col3, COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1 GROUP BY t1.col3""".format(
            table_schema, left_dist, right_dist
        )
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual[row[0]] = row[1] 
    elif query_type == "sum":  # sum
        actual_sql = """SELECT t1.col3, SUM(t1.col2) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1 GROUP BY t1.col3""".format(
            table_schema, left_dist, right_dist
        )
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual[row[0]] = row[1] 
    elif query_type == "avg":  # avg
        actual_sql = """SELECT t1.col3, AVG(t1.col2) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON t1.col1 = t2.col1 GROUP BY t1.col3""".format(
            table_schema, left_dist, right_dist
        )
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual[row[0]] = row[1] 
    else:
        print("Unsupported query: {}".format(query_type))
        return
    cur.close()

    # get start idx
    cur = conn.cursor()
    sql = """SELECT max(idx) FROM {}.strat_results WHERE query = '{}' AND left_dist = '{}' AND
    right_dist = '{}' and query = '{}' GROUP BY query, left_dist, right_dist""".format(
        sample_schema, query_type, left_dist, right_dist, query_type
    )
    cur.execute(sql)
    result = cur.fetchall()
    start_idx = 0
    for row in result:
        start_idx = row[0] if row[0] is not None else 0
    start_idx = start_idx + 1
    cur.close()

    cur = conn.cursor()
    group_info_table = "{}__{}__groupinfo".format(left_dist, right_dist)

    # get estimate for each sample pair
    for i in range(start_idx, num_sample + 1):
        estimates = np.zeros((10))
        S1_name = "s__{}__{}__{}K__{:.3f}q_{}_{}".format(left_dist, right_dist, K, q, i, 1)
        S2_name = "s__{}__{}__{}K__{:.3f}q_{}_{}".format(left_dist, right_dist, K, q, i, 2)
        S1_name = S1_name.replace(".", "_")
        S2_name = S2_name.replace(".", "_")
        cur = conn.cursor()
        if query_type == "count":  # count
            estimate_sql = """
            select col3 as grp, case when col3 < 2 then c * (1/(q*q)) else c * (n/k) * (1/q) end as estimate from
            (select t1.col3 as col3, count(*) c, sum(t1.col2) as s from
            {0}.{1} t1 join
            {0}.{2} t2 on t1.col1 = t2.col1
            group by t1.col3) t1 join 
            (select distinct groupid, k, n, q from
            {0}.{3}) ginfo
            on t1.col3 = ginfo.groupid
            order by col3;
            """.format(
                sample_schema, S1_name, S2_name, group_info_table
            )
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                grp = row[0]
                estimate = row[1] if row[1] is not None else 0
                estimates[row[0]] = estimate

        elif query_type == "sum":  # sum
            estimate_sql = """
            select col3 as grp, case when col3 < 2 then s * (1/(q*q)) else s * (n/k) * (1/q) end as estimate from
            (select t1.col3 as col3, count(*) c, sum(t1.col2) as s from
            {0}.{1} t1 join
            {0}.{2} t2 on t1.col1 = t2.col1
            group by t1.col3) t1 join 
            (select distinct groupid, k, n, q from
            {0}.{3}) ginfo
            on t1.col3 = ginfo.groupid
            order by col3;
            """.format(
                sample_schema, S1_name, S2_name, group_info_table
            )
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                grp = row[0]
                estimate = row[1] if row[1] is not None else 0
                estimates[row[0]] = estimate
        elif query_type == "avg":  # avg
            estimate_sql = """
            select col3 as grp, s/c as estimate from
            (select t1.col3 as col3, count(*) c, sum(t1.col2) as s from
            {0}.{1} t1 join
            {0}.{2} t2 on t1.col1 = t2.col1
            group by t1.col3) t1 join 
            (select distinct groupid, k, n, q from
            {0}.{3}) ginfo
            on t1.col3 = ginfo.groupid
            order by col3;
            """.format(
                sample_schema, S1_name, S2_name, group_info_table
            )
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                grp = row[0]
                estimate = row[1] if row[1] is not None else 0
                estimates[row[0]] = estimate
        cur.close()

        # record result
        cur = conn.cursor()
        for grp in range(0, 10):
            if estimates[grp] != 0:
                add_result_sql = """INSERT INTO TABLE {}.{} VALUES (
                '{}', {}, '{}', '{}', {}, {}, {}, now()
                )""".format(
                    sample_schema,
                    temp_table,
                    query_type,
                    i,
                    left_dist,
                    right_dist,
                    grp,
                    actual[grp],
                    estimates[grp],
                )
                cur.execute(add_result_sql)
        cur.close()

    cur = conn.cursor()
    cur.execute(
        "INSERT INTO TABLE {0}.strat_results SELECT * FROM {0}.{1}".format(
            sample_schema, temp_table
        )
    )
    conn.commit()
    cur.execute("DROP TABLE IF EXISTS {}.{}".format(sample_schema, temp_table))
    conn.commit()
    cur.close()


def run_instacart_ours(
    host, port, table_schema, sample_schema, query_type, num_sample, overwrite=False
):
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
    LIMIT 1""".format(
        sample_schema, query_type
    )
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
    if query_type == "count":  # count
        actual_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
        t1.order_id = t2.order_id
        WHERE t1.order_hour_of_day <> 1""".format(
            table_schema, "orders", "order_products"
        )
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == "sum":  # sum
        actual_sql = """SELECT SUM(t1.days_since_prior) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
        t1.order_id = t2.order_id
        WHERE t1.order_hour_of_day <> 1""".format(
            table_schema, "orders", "order_products"
        )
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == "avg":  # avg
        actual_sql = """SELECT AVG(t1.order_dow) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
        t1.order_id = t2.order_id
        WHERE t1.order_hour_of_day <> 1""".format(
            table_schema, "orders", "order_products"
        )
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
        sample_schema, query_type, "orders", "order_products"
    )
    cur.execute(sql)
    result = cur.fetchall()
    start_idx = 0
    for row in result:
        start_idx = row[0] if row[0] is not None else 0
    start_idx = start_idx + 1
    cur.close()

    # get estimate for each sample pair
    for i in range(start_idx, num_sample + 1):
        S1_name = "s__{}__{}__{}__{}__{}__{}__{}".format(
            "orders", "order_products", t1_join_col, t1_agg_col, query_type, i, 1
        )
        S2_name = "s__{}__{}__{}__{}__{}__{}".format(
            "orders", "order_products", t2_join_col, query_type, i, 2
        )

        cur = conn.cursor()
        if query_type == "count":  # count
            estimate_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.order_id = t2.order_id
            WHERE t1.order_hour_of_day <> 1""".format(
                sample_schema, S1_name, S2_name
            )
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = estimate * (1 / (p * q * q))
        elif query_type == "sum":  # sum
            estimate_sql = """SELECT SUM(t1.days_since_prior) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.order_id = t2.order_id
            WHERE t1.order_hour_of_day <> 1""".format(
                sample_schema, S1_name, S2_name
            )
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = float(estimate) * (1 / (p * q * q))
        elif query_type == "avg":  # avg
            estimate_sql = """SELECT COUNT(*) as val1, SUM(t1.order_dow) as val2 FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.order_id = t2.order_id
            WHERE t1.order_hour_of_day <> 1""".format(
                sample_schema, S1_name, S2_name
            )
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
        )""".format(
            sample_schema,
            temp_table,
            query_type,
            i,
            "orders",
            "order_products",
            p,
            q,
            actual,
            estimate,
        )
        cur.execute(add_result_sql)
        cur.close()

    cur = conn.cursor()
    cur.execute(
        "INSERT INTO TABLE {0}.results SELECT * FROM {0}.{1}".format(
            sample_schema, temp_table
        )
    )
    conn.commit()
    cur.execute("DROP TABLE IF EXISTS {}.{}".format(sample_schema, temp_table))
    conn.commit()
    cur.close()


def run_instacart_ours_with_where(
    host,
    port,
    table_schema,
    sample_schema,
    query_type,
    cond_type,
    where_dist,
    num_sample,
    overwrite=False,
):
    conn = impaladb.connect(host, port)
    if overwrite:
        drop_where_result_table(conn, sample_schema)

    # create result table
    create_where_result_table(conn, sample_schema)
    # create temp table
    temp_table = create_temp_table(conn, sample_schema, "where_results")
    result_table = "where_results"

    # get p,q,t1_join_col, t2_join_col, t1_agg_col
    sql = """SELECT p, q, t1_join_col, t2_join_col, t1_agg_col FROM {}.meta WHERE
    agg = '{}' and cond_type = '{}' and where_dist = '{}' ORDER BY ts DESC
    LIMIT 1""".format(
        sample_schema, query_type, cond_type, where_dist
    )
    cur = conn.cursor()
    cur.execute(sql)
    result = cur.fetchall()
    for row in result:
        p = row[0]
        q = row[1]
        t1_join_col = row[2]
        t2_join_col = row[3]
        t1_agg_col = row[4]

    if cond_type == "eq":
        cond_op = "="
    elif cond_type == "geq":
        cond_op = ">="
    else:
        print("Unsupported Cond Op: {}".format(cond_type))
        return

    cur = conn.cursor()
    cur.execute(
        "SELECT MIN({0}), MAX({0}) FROM {1}.{2} t1".format(
            "order_hour_of_day", table_schema, "orders"
        )
    )
    row = cur.fetchone()
    start_val = row[0]
    end_val = row[1]
    increment = 1

    for cond_val in range(start_val, end_val + 1, increment):
        where_str = "{} {}".format(cond_op, cond_val)

        # get actual value first
        cur = conn.cursor()
        if query_type == "count":  # count
            actual_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.order_id = t2.order_id
            WHERE t1.order_hour_of_day {3} {4}""".format(
                table_schema, "orders", "order_products", cond_op, cond_val
            )
            cur.execute(actual_sql)
            result = cur.fetchall()
            for row in result:
                actual = row[0] if row[0] is not None else 0
        elif query_type == "sum":  # sum
            actual_sql = """SELECT SUM(t1.days_since_prior) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.order_id = t2.order_id
            WHERE t1.order_hour_of_day {3} {4}""".format(
                table_schema, "orders", "order_products", cond_op, cond_val
            )
            cur.execute(actual_sql)
            result = cur.fetchall()
            for row in result:
                actual = row[0] if row[0] is not None else 0
        elif query_type == "avg":  # avg
            actual_sql = """SELECT AVG(t1.order_dow) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.order_id = t2.order_id
            WHERE t1.order_hour_of_day {3} {4}""".format(
                table_schema, "orders", "order_products", cond_op, cond_val
            )
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
        sql = """SELECT max(idx) FROM {}.{} WHERE query = '{}' AND left_dist = '{}' AND
        right_dist = '{}' AND cond_type = '{}' AND cond_val = {} AND where_dist = '{}'
        GROUP BY query, left_dist, right_dist, cond_type, cond_val, where_dist""".format(
            sample_schema,
            result_table,
            query_type,
            "orders",
            "order_products",
            cond_type,
            cond_val,
            where_dist,
        )
        cur.execute(sql)
        result = cur.fetchall()
        start_idx = 0
        for row in result:
            start_idx = row[0] if row[0] is not None else 0
        start_idx = start_idx + 1
        cur.close()

        # get estimate for each sample pair
        for i in range(start_idx, num_sample + 1):
            S1_name = "s__{}__{}__{}__{}__{}__{}__{}__{}__{}".format(
                "orders",
                "order_products",
                t1_join_col,
                t1_agg_col,
                query_type,
                cond_type,
                where_dist,
                i,
                1,
            )
            S2_name = "s__{}__{}__{}__{}__{}__{}__{}__{}".format(
                "orders",
                "order_products",
                t2_join_col,
                query_type,
                cond_type,
                where_dist,
                i,
                2,
            )

            cur = conn.cursor()
            if query_type == "count":  # count
                estimate_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
                t1.order_id = t2.order_id
                WHERE t1.order_hour_of_day {3}""".format(
                    sample_schema, S1_name, S2_name, where_str
                )
                cur.execute(estimate_sql)
                result = cur.fetchall()
                for row in result:
                    estimate = row[0] if row[0] is not None else 0
                estimate = estimate * (1 / (p * q * q))
            elif query_type == "sum":  # sum
                estimate_sql = """SELECT SUM(t1.days_since_prior) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
                t1.order_id = t2.order_id
                WHERE t1.order_hour_of_day {3}""".format(
                    sample_schema, S1_name, S2_name, where_str
                )
                cur.execute(estimate_sql)
                result = cur.fetchall()
                for row in result:
                    estimate = row[0] if row[0] is not None else 0
                estimate = float(estimate) * (1 / (p * q * q))
            elif query_type == "avg":  # avg
                estimate_sql = """SELECT COUNT(*) as val1, SUM(t1.order_dow) as val2 FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
                t1.order_id = t2.order_id
                WHERE t1.order_hour_of_day {3}""".format(
                    sample_schema, S1_name, S2_name, where_str
                )
                cur.execute(estimate_sql)
                result = cur.fetchall()
                for row in result:
                    estimate_count = row[0] if row[0] is not None else 0
                    estimate_sum = row[1] if row[1] is not None else 0
                estimate_sum = estimate_sum * (1 / (p * q * q))
                estimate_count = estimate_count * (1 / (p * q * q))
                estimate = estimate_sum / estimate_count if estimate_count != 0 else 0

            # record result
            add_result_sql = """INSERT INTO TABLE {}.{} VALUES (
            '{}', {}, '{}', '{}', '{}', {}, '{}', {:.3f}, {:.3f}, {}, {}, now()
            )""".format(
                sample_schema,
                temp_table,
                query_type,
                i,
                "orders",
                "order_products",
                cond_type,
                cond_val,
                where_dist,
                p,
                q,
                actual,
                estimate,
            )
            cur.execute(add_result_sql)

    cur = conn.cursor()
    cur.execute(
        "INSERT INTO TABLE {0}.{2} SELECT * FROM {0}.{1}".format(
            sample_schema, temp_table, result_table
        )
    )
    conn.commit()
    cur.execute("DROP TABLE IF EXISTS {}.{}".format(sample_schema, temp_table))
    conn.commit()
    cur.close()


def run_instacart_preset(
    host,
    port,
    table_schema,
    sample_schema,
    query_type,
    p,
    q,
    num_sample,
    overwrite=False,
):
    conn = impaladb.connect(host, port)
    if overwrite:
        drop_result_table(conn, sample_schema)

    # create result table
    create_result_table(conn, sample_schema)
    # create temp table
    temp_table = create_temp_table(conn, sample_schema)

    # get actual value first
    cur = conn.cursor()
    if query_type == "count":  # count
        actual_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
        t1.order_id = t2.order_id
        WHERE t1.order_hour_of_day <> 1""".format(
            table_schema, "orders", "order_products"
        )
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == "sum":  # sum
        actual_sql = """SELECT SUM(t1.days_since_prior) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
        t1.order_id = t2.order_id
        WHERE t1.order_hour_of_day <> 1""".format(
            table_schema, "orders", "order_products"
        )
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == "avg":  # avg
        actual_sql = """SELECT AVG(t1.order_dow) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
        t1.order_id = t2.order_id
        WHERE t1.order_hour_of_day <> 1""".format(
            table_schema, "orders", "order_products"
        )
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
        sample_schema, query_type, "orders", "order_products", p, q
    )
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
            "orders", "order_products", p, q, i, 1
        )
        S2_name = "s__{}__{}__{:.3f}p__{:.3f}q_{}_{}".format(
            "orders", "order_products", p, q, i, 2
        )
        S1_name = S1_name.replace(".", "_")
        S2_name = S2_name.replace(".", "_")
        cur = conn.cursor()
        if query_type == "count":  # count
            estimate_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.order_id = t2.order_id
            WHERE t1.order_hour_of_day <> 1""".format(
                sample_schema, S1_name, S2_name
            )
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = estimate * (1 / (p * q * q))
        elif query_type == "sum":  # sum
            estimate_sql = """SELECT SUM(t1.days_since_prior) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.order_id = t2.order_id
            WHERE t1.order_hour_of_day <> 1""".format(
                sample_schema, S1_name, S2_name
            )
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = float(estimate) * (1 / (p * q * q))
        elif query_type == "avg":  # avg
            estimate_sql = """SELECT COUNT(*) as val1, SUM(t1.order_dow) as val2 FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.order_id = t2.order_id
            WHERE t1.order_hour_of_day <> 1""".format(
                sample_schema, S1_name, S2_name
            )
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
        )""".format(
            sample_schema,
            temp_table,
            query_type,
            i,
            "orders",
            "order_products",
            p,
            q,
            actual,
            estimate,
        )
        cur.execute(add_result_sql)
        cur.close()

    cur = conn.cursor()
    cur.execute(
        "INSERT INTO TABLE {0}.results SELECT * FROM {0}.{1}".format(
            sample_schema, temp_table
        )
    )
    conn.commit()
    cur.execute("DROP TABLE IF EXISTS {}.{}".format(sample_schema, temp_table))
    conn.commit()
    cur.close()


def run_instacart_preset_with_where(
    host,
    port,
    table_schema,
    sample_schema,
    query_type,
    cond_type,
    p,
    q,
    num_sample,
    overwrite=False,
):
    conn = impaladb.connect(host, port)
    if overwrite:
        drop_where_result_table(conn, sample_schema)

    # create result table
    create_where_result_table(conn, sample_schema)
    # create temp table
    temp_table = create_temp_table(conn, sample_schema, "where_results")
    result_table = "where_results"

    if cond_type == "eq":
        cond_op = "="
    elif cond_type == "geq":
        cond_op = ">="
    else:
        print("Unsupported Cond Op: {}".format(cond_type))
        return

    cur = conn.cursor()
    cur.execute(
        "SELECT MIN({0}), MAX({0}) FROM {1}.{2} t1".format(
            "order_hour_of_day", table_schema, "orders"
        )
    )
    row = cur.fetchone()
    start_val = row[0]
    end_val = row[1]
    increment = 1

    for cond_val in range(start_val, end_val + 1, increment):
        where_str = "{} {}".format(cond_op, cond_val)
        # get actual value first
        cur = conn.cursor()
        if query_type == "count":  # count
            actual_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.order_id = t2.order_id
            WHERE t1.order_hour_of_day {3}""".format(
                table_schema, "orders", "order_products", where_str
            )
            cur.execute(actual_sql)
            result = cur.fetchall()
            for row in result:
                actual = row[0] if row[0] is not None else 0
        elif query_type == "sum":  # sum
            actual_sql = """SELECT SUM(t1.days_since_prior) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.order_id = t2.order_id
            WHERE t1.order_hour_of_day {3}""".format(
                table_schema, "orders", "order_products", where_str
            )
            cur.execute(actual_sql)
            result = cur.fetchall()
            for row in result:
                actual = row[0] if row[0] is not None else 0
        elif query_type == "avg":  # avg
            actual_sql = """SELECT AVG(t1.order_dow) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.order_id = t2.order_id
            WHERE t1.order_hour_of_day {3}""".format(
                table_schema, "orders", "order_products", where_str
            )
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
        sql = """SELECT max(idx) FROM {}.{} WHERE query = '{}' AND left_dist = '{}' AND
        right_dist = '{}' and p = {} and q = {} and cond_type = '{}' and cond_val = {}
        GROUP BY query, left_dist, right_dist, p, q, cond_type, cond_val""".format(
            sample_schema,
            result_table,
            query_type,
            "orders",
            "order_products",
            p,
            q,
            cond_type,
            cond_val,
        )
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
                "orders", "order_products", p, q, i, 1
            )
            S2_name = "s__{}__{}__{:.3f}p__{:.3f}q_{}_{}".format(
                "orders", "order_products", p, q, i, 2
            )
            S1_name = S1_name.replace(".", "_")
            S2_name = S2_name.replace(".", "_")
            cur = conn.cursor()
            if query_type == "count":  # count
                estimate_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
                t1.order_id = t2.order_id
                WHERE t1.order_hour_of_day {3}""".format(
                    sample_schema, S1_name, S2_name, where_str
                )
                cur.execute(estimate_sql)
                result = cur.fetchall()
                for row in result:
                    estimate = row[0] if row[0] is not None else 0
                estimate = estimate * (1 / (p * q * q))
            elif query_type == "sum":  # sum
                estimate_sql = """SELECT SUM(t1.days_since_prior) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
                t1.order_id = t2.order_id
                WHERE t1.order_hour_of_day {3}""".format(
                    sample_schema, S1_name, S2_name, where_str
                )
                cur.execute(estimate_sql)
                result = cur.fetchall()
                for row in result:
                    estimate = row[0] if row[0] is not None else 0
                estimate = float(estimate) * (1 / (p * q * q))
            elif query_type == "avg":  # avg
                estimate_sql = """SELECT COUNT(*) as val1, SUM(t1.order_dow) as val2 FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
                t1.order_id = t2.order_id
                WHERE t1.order_hour_of_day {3}""".format(
                    sample_schema, S1_name, S2_name, where_str
                )
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
            '{}', {}, '{}', '{}', '{}', {}, '{}', {:.3f}, {:.3f}, {}, {}, now()
            )""".format(
                sample_schema,
                temp_table,
                query_type,
                i,
                "orders",
                "order_products",
                cond_type,
                cond_val,
                "N/A",
                p,
                q,
                actual,
                estimate,
            )
            cur.execute(add_result_sql)
            cur.close()

    cur = conn.cursor()
    cur.execute(
        "INSERT INTO TABLE {0}.{2} SELECT * FROM {0}.{1}".format(
            sample_schema, temp_table, result_table
        )
    )
    conn.commit()
    cur.execute("DROP TABLE IF EXISTS {}.{}".format(sample_schema, temp_table))
    conn.commit()
    cur.close()


def run_movielens_ours(
    host, port, table_schema, sample_schema, query_type, num_sample, overwrite=False
):
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
    LIMIT 1""".format(
        sample_schema, query_type
    )
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
    if query_type == "count":  # count
        actual_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
        t1.movieid = t2.movieid
        WHERE t2.genres NOT LIKE '%Documentary%'""".format(
            table_schema, "ratings", "movies"
        )
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == "sum":  # sum
        actual_sql = """SELECT SUM(rating) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
        t1.movieid = t2.movieid
        WHERE t2.genres NOT LIKE '%Documentary%'""".format(
            table_schema, "ratings", "movies"
        )
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == "avg":  # avg
        actual_sql = """SELECT AVG(rating) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
        t1.movieid = t2.movieid
        WHERE t2.genres NOT LIKE '%Documentary%'""".format(
            table_schema, "ratings", "movies"
        )
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
        sample_schema, query_type, "ratings", "movies"
    )
    cur.execute(sql)
    result = cur.fetchall()
    start_idx = 0
    for row in result:
        start_idx = row[0] if row[0] is not None else 0
    start_idx = start_idx + 1
    cur.close()

    # get estimate for each sample pair
    for i in range(start_idx, num_sample + 1):
        S1_name = "s__{}__{}__{}__{}__{}__{}__{}".format(
            "ratings", "movies", t1_join_col, t1_agg_col, query_type, i, 1
        )
        S2_name = "s__{}__{}__{}__{}__{}__{}".format(
            "ratings", "movies", t2_join_col, query_type, i, 2
        )
        cur = conn.cursor()
        if query_type == "count":  # count
            estimate_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.movieid = t2.movieid
            WHERE t2.genres NOT LIKE '%Documentary%'""".format(
                sample_schema, S1_name, S2_name
            )
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = estimate * (1 / (p * q * q))
        elif query_type == "sum":  # sum
            estimate_sql = """SELECT SUM(rating) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.movieid = t2.movieid
            WHERE t2.genres NOT LIKE '%Documentary%'""".format(
                sample_schema, S1_name, S2_name
            )
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = float(estimate) * (1 / (p * q * q))
        elif query_type == "avg":  # avg
            estimate_sql = """SELECT COUNT(*) as val1, SUM(rating) as val2 FROM
            {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.movieid = t2.movieid
            WHERE t2.genres NOT LIKE '%Documentary%'""".format(
                sample_schema, S1_name, S2_name
            )
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
        )""".format(
            sample_schema,
            temp_table,
            query_type,
            i,
            "ratings",
            "movies",
            p,
            q,
            actual,
            estimate,
        )
        cur.execute(add_result_sql)
        cur.close()

    cur = conn.cursor()
    cur.execute(
        "INSERT INTO TABLE {0}.results SELECT * FROM {0}.{1}".format(
            sample_schema, temp_table
        )
    )
    conn.commit()
    cur.execute("DROP TABLE IF EXISTS {}.{}".format(sample_schema, temp_table))
    conn.commit()
    cur.close()


def run_movielens_preset(
    host,
    port,
    table_schema,
    sample_schema,
    query_type,
    p,
    q,
    num_sample,
    overwrite=False,
):
    conn = impaladb.connect(host, port)
    if overwrite:
        drop_result_table(conn, sample_schema)

    # create result table
    create_result_table(conn, sample_schema)
    # create temp table
    temp_table = create_temp_table(conn, sample_schema)

    # get actual value first
    cur = conn.cursor()
    if query_type == "count":  # count
        actual_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
        t1.movieid = t2.movieid
        WHERE t2.genres NOT LIKE '%Documentary%'""".format(
            table_schema, "ratings", "movies"
        )
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == "sum":  # sum
        actual_sql = """SELECT SUM(rating) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
        t1.movieid = t2.movieid
        WHERE t2.genres NOT LIKE '%Documentary%'""".format(
            table_schema, "ratings", "movies"
        )
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == "avg":  # avg
        actual_sql = """SELECT AVG(rating) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
        t1.movieid = t2.movieid
        WHERE t2.genres NOT LIKE '%Documentary%'""".format(
            table_schema, "ratings", "movies"
        )
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
        sample_schema, query_type, "ratings", "movies", p, q
    )
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
            "movies", "ratings", p, q, i, 1
        )
        S2_name = "s__{}__{}__{:.3f}p__{:.3f}q_{}_{}".format(
            "movies", "ratings", p, q, i, 2
        )
        S1_name = S1_name.replace(".", "_")
        S2_name = S2_name.replace(".", "_")
        cur = conn.cursor()
        if query_type == "count":  # count
            estimate_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.movieid = t2.movieid
            WHERE t1.genres NOT LIKE '%Documentary%'""".format(
                sample_schema, S1_name, S2_name
            )
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = estimate * (1 / (p * q * q))
        elif query_type == "sum":  # sum
            estimate_sql = """SELECT SUM(rating) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.movieid = t2.movieid
            WHERE t1.genres NOT LIKE '%Documentary%'""".format(
                sample_schema, S1_name, S2_name
            )
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = float(estimate) * (1 / (p * q * q))
        elif query_type == "avg":  # avg
            estimate_sql = """SELECT COUNT(*) as val1, SUM(rating) as val2 FROM
            {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.movieid = t2.movieid
            WHERE t1.genres NOT LIKE '%Documentary%'""".format(
                sample_schema, S1_name, S2_name
            )
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
        )""".format(
            sample_schema,
            temp_table,
            query_type,
            i,
            "ratings",
            "movies",
            p,
            q,
            actual,
            estimate,
        )
        cur.execute(add_result_sql)
        cur.close()

    cur = conn.cursor()
    cur.execute(
        "INSERT INTO TABLE {0}.results SELECT * FROM {0}.{1}".format(
            sample_schema, temp_table
        )
    )
    conn.commit()
    cur.execute("DROP TABLE IF EXISTS {}.{}".format(sample_schema, temp_table))
    conn.commit()
    cur.close()


def run_tpch_ours(
    host, port, table_schema, sample_schema, query_type, num_sample, overwrite=False
):
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
    LIMIT 1""".format(
        sample_schema, query_type
    )
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
    if query_type == "count":  # count
        actual_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
        t1.l_orderkey = t2.o_orderkey
        WHERE t2.o_orderstatus <> 'P'""".format(
            table_schema, "lineitem", "orders"
        )
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == "sum":  # sum
        actual_sql = """SELECT SUM(l_quantity) FROM {0}.{1} t1
        JOIN {0}.{2} t2 ON
        t1.l_orderkey = t2.o_orderkey
        WHERE t2.o_orderstatus <> 'P'""".format(
            table_schema, "lineitem", "orders"
        )
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == "avg":  # avg
        actual_sql = """SELECT AVG(l_extendedprice) FROM {0}.{1} t1
        JOIN {0}.{2} t2 ON
        t1.l_orderkey = t2.o_orderkey
        WHERE t2.o_orderstatus <> 'P'""".format(
            table_schema, "lineitem", "orders"
        )
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
        sample_schema, query_type, "lineitem", "orders"
    )
    cur.execute(sql)
    result = cur.fetchall()
    start_idx = 0
    for row in result:
        start_idx = row[0] if row[0] is not None else 0
    start_idx = start_idx + 1
    cur.close()

    # get estimate for each sample pair
    for i in range(start_idx, num_sample + 1):
        S1_name = "s__{}__{}__{}__{}__{}__{}__{}".format(
            "lineitem", "orders", t1_join_col, t1_agg_col, query_type, i, 1
        )
        S2_name = "s__{}__{}__{}__{}__{}__{}".format(
            "lineitem", "orders", t2_join_col, query_type, i, 2
        )
        cur = conn.cursor()
        if query_type == "count":  # count
            estimate_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.l_orderkey = t2.o_orderkey
            WHERE t2.o_orderstatus <> 'P'""".format(
                sample_schema, S1_name, S2_name
            )
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = estimate * (1 / (p * q * q))
        elif query_type == "sum":  # sum
            estimate_sql = """SELECT SUM(l_quantity) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.l_orderkey = t2.o_orderkey
            WHERE t2.o_orderstatus <> 'P'""".format(
                sample_schema, S1_name, S2_name
            )
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = float(estimate) * (1 / (p * q * q))
        elif query_type == "avg":  # avg
            estimate_sql = """SELECT COUNT(*) as val1, SUM(l_extendedprice) as val2
            FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.l_orderkey = t2.o_orderkey
            WHERE t2.o_orderstatus <> 'P'""".format(
                sample_schema, S1_name, S2_name
            )
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
        )""".format(
            sample_schema,
            temp_table,
            query_type,
            i,
            "lineitem",
            "orders",
            p,
            q,
            actual,
            estimate,
        )
        cur.execute(add_result_sql)
        cur.close()

    cur = conn.cursor()
    cur.execute(
        "INSERT INTO TABLE {0}.results SELECT * FROM {0}.{1}".format(
            sample_schema, temp_table
        )
    )
    conn.commit()
    cur.execute("DROP TABLE IF EXISTS {}.{}".format(sample_schema, temp_table))
    conn.commit()
    cur.close()


def run_tpch_preset(
    host,
    port,
    table_schema,
    sample_schema,
    query_type,
    p,
    q,
    num_sample,
    overwrite=False,
):
    conn = impaladb.connect(host, port)
    if overwrite:
        drop_result_table(conn, sample_schema)

    # create result table
    create_result_table(conn, sample_schema)
    # create temp table
    temp_table = create_temp_table(conn, sample_schema)

    # get actual value first
    cur = conn.cursor()
    if query_type == "count":  # count
        actual_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
        t1.l_orderkey = t2.o_orderkey
        WHERE t2.o_orderstatus <> 'P'""".format(
            table_schema, "lineitem", "orders"
        )
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == "sum":  # sum
        actual_sql = """SELECT SUM(l_quantity) FROM {0}.{1} t1
        JOIN {0}.{2} t2 ON
        t1.l_orderkey = t2.o_orderkey
        WHERE t2.o_orderstatus <> 'P'""".format(
            table_schema, "lineitem", "orders"
        )
        cur.execute(actual_sql)
        result = cur.fetchall()
        for row in result:
            actual = row[0] if row[0] is not None else 0
    elif query_type == "avg":  # avg
        actual_sql = """SELECT AVG(l_extendedprice) FROM {0}.{1} t1
        JOIN {0}.{2} t2 ON
        t1.l_orderkey = t2.o_orderkey
        WHERE t2.o_orderstatus <> 'P'""".format(
            table_schema, "lineitem", "orders"
        )
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
        sample_schema, query_type, "lineitem", "orders", p, q
    )
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
            "orders", "lineitem", p, q, i, 1
        )
        S2_name = "s__{}__{}__{:.3f}p__{:.3f}q_{}_{}".format(
            "orders", "lineitem", p, q, i, 2
        )
        S1_name = S1_name.replace(".", "_")
        S2_name = S2_name.replace(".", "_")
        cur = conn.cursor()
        if query_type == "count":  # count
            estimate_sql = """SELECT COUNT(*) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.o_orderkey = t2.l_orderkey
            WHERE t1.o_orderstatus <> 'P'""".format(
                sample_schema, S1_name, S2_name
            )
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = estimate * (1 / (p * q * q))
        elif query_type == "sum":  # sum
            estimate_sql = """SELECT SUM(l_quantity) FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.o_orderkey = t2.l_orderkey
            WHERE t1.o_orderstatus <> 'P'""".format(
                sample_schema, S1_name, S2_name
            )
            cur.execute(estimate_sql)
            result = cur.fetchall()
            for row in result:
                estimate = row[0] if row[0] is not None else 0
            estimate = float(estimate) * (1 / (p * q * q))
        elif query_type == "avg":  # avg
            estimate_sql = """SELECT COUNT(*) as val1, SUM(l_extendedprice) as val2
            FROM {0}.{1} t1 JOIN {0}.{2} t2 ON
            t1.o_orderkey = t2.l_orderkey
            WHERE t1.o_orderstatus <> 'P'""".format(
                sample_schema, S1_name, S2_name
            )
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
        )""".format(
            sample_schema,
            temp_table,
            query_type,
            i,
            "lineitem",
            "orders",
            p,
            q,
            actual,
            estimate,
        )
        cur.execute(add_result_sql)
        cur.close()

    cur = conn.cursor()
    cur.execute(
        "INSERT INTO TABLE {0}.results SELECT * FROM {0}.{1}".format(
            sample_schema, temp_table
        )
    )
    conn.commit()
    cur.execute("DROP TABLE IF EXISTS {}.{}".format(sample_schema, temp_table))
    conn.commit()
    cur.close()
