import impala.dbapi as impaladb

def get_var(conn, schema):
    cur = conn.cursor()
    cur2 = conn.cursor()
    cur.execute("SHOW TABLES IN {} like '*0_010p__1_000q*'".format(schema))
    results = cur.fetchall()
    table_count = []
    for row in results:


        table_list.append(row[0])

    for table in table_list:
        sql = "DROP TABLE IF EXISTS {}.{} PURGE".format(schema, table)
        print(sql)
        cur.execute(sql)
    cur.execute("DROP SCHEMA IF EXISTS {} CASCADE".format(schema))
    conn.commit()

conn = impaladb.connect('cp-5', 21050)
drop_schema(conn, 'synthetic_10m_preset')
