import math
import time

import impala.dbapi as impaladb
import numpy as np

hdfs_dir = '/tmp/approxjoin'
raw_data_path = './raw_data'
text_schema = 'approxjoin_text'
parquet_schema = 'approxjoin_parquet'
sample_schema = 'approxjoin_parquet_sample'
impala_host = 'cp-2'
impala_port = 21050
e1 = 0.01
e2 = 0.01


def reset_schema():
    conn = impaladb.connect(impala_host, impala_port)
    cur = conn.cursor()
    cur.execute("DROP SCHEMA IF EXISTS {0} CASCADE".format(sample_schema))
    cur.execute("CREATE SCHEMA IF NOT EXISTS {0}".format(sample_schema))


def create_sample(num_rows,
                  num_keys,
                  leftDist,
                  rightDist,
                  type,
                  isCentralized=True):

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

    keys = np.arange(1, num_keys + 1)
    a_v[:, 0] = keys
    b_v[:, 0] = keys
    mu_v[:, 0] = keys
    var_v[:, 0] = keys

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
            val = math.sqrt(e1 * e2 * sum1 / sum2)
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
