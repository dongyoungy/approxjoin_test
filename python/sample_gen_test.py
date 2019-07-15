import timeit
import sample_gen as sg
import data_gen as dg
import multiprocessing as mp
import calculate_agg as cal
from itertools import product

setup = "import sample_gen as sg"

arg_list = []
#  arg_list.append(
#  "'tpch100g_parquet', 'lineitem', 'l_orderkey', 'l_quantity', 'tpch100g_parquet', 'orders', 'o_orderkey'"
#  )
arg_list.append(
"'tpch300g_parquet', 'orders', 'o_orderkey', 'o_totalprice', 'tpch300g_parquet', 'lineitem', 'l_orderkey', None"
)
#  arg_list.append(
#  "'tpch100g_parquet', 'lineitem', 'l_partkey', 'l_quantity', 'tpch500g_parquet', 'lineitem', 'l_partkey'"
#  )
#  arg_list.append(
#  "'tpch100g_parquet', 'orders', 'o_orderkey', 'o_totalprice', 'tpch500g_parquet', 'orders', 'o_orderkey'"
#  )
#  arg_list.append(
#  "'tpch100g_parquet', 'orders', 'o_orderkey', 'o_totalprice', 'tpch100g_parquet', 'lineitem', 'l_orderkey', \"year(o_orderdate) = 1992 and o_orderpriority = \'1-URGENT\'\""
#  )
#  arg_list.append(
    #  "'tpch500g_parquet', 'orders', 'o_orderkey', 'o_totalprice', 'tpch500g_parquet', 'lineitem', 'l_orderkey', \"year(o_orderdate) = 1992 and o_orderpriority = \'1-URGENT\' and o_totalprice < 1000\""
#  )

for arg in arg_list:
    for type in ['count']:
        #  for type in ['count', 'sum']:
        for is_centralized in [True, False]:
            sg.reset_schema()
            s = "sg.create_sample_pair_from_database({}, '{}', {})".format(
                arg, type, is_centralized)
            d = ''
            if is_centralized:
                d = 'cent'
            else:
                d = 'dec'
            t = timeit.timeit(stmt=s, setup=setup, number=1)
            print("[{}, {}, {}] = {} s".format(arg, type, d, t), flush=True)
