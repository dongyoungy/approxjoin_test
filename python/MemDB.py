import sqlite3


class MemDB:
    def __init__(self):
        self.db = sqlite3.connect("file::memory:")

    def create_table(self, table_name, cols, data_df):
        self.execute("DROP TABLE IF EXISTS {}".format(table_name))
        self.db.commit()
        data_df.columns = cols
        data_df.to_sql(table_name, self.db, if_exists='append', index=False)
        self.db.commit()

    def execute(self, query):
        cur = self.db.cursor()
        cur.execute(query)
        self.db.commit()
        return cur
