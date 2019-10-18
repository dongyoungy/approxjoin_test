import impala.dbapi as impaladb

def drop_schema(conn, schema):
    cur = conn.cursor()
    cur.execute("SHOW TABLES IN {}".format(schema))
    results = cur.fetchall()
    table_list = []
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
