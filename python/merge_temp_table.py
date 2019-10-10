import impala.dbapi as impaladb

impala_host = "cp-2"
impala_port = 21050

conn = impaladb.connect(impala_host, impala_port)
cur = conn.cursor()
cur2 = conn.cursor()

schema = "synthetic_10m_preset2"

cur.execute("SHOW TABLES IN {} LIKE 'result_temp_*'".format(schema))
results = cur.fetchall()
for row in results:
    sql = """ INSERT INTO TABLE {0}.where_results 
        select r1.* from
    {0}.{1} r1 left join
    {0}.where_results r2 on r1.query = r2.query and r1.idx = r2.idx and r1.cond_val = r2.cond_val and r1.where_dist = r2.where_dist
    and r1.p = r2.p and r1.q = r2.q
    where r2.idx is null;
    """.format(
        schema, row[0]
    )
    print(sql, flush=True)
    cur2.execute(sql)

