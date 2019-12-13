import data_gen as dg
import multiprocessing as mp

args = []
where_args = []
max_var_args = []
num_proc = 16
pool = mp.Pool(processes=num_proc, maxtasksperchild=10)

for dist in ["uniform1", "uniform2", "normal1", "powerlaw1", "normal2", "powerlaw2"]:
    args.append((100 * 1000 * 1000, 10 * 1000, dist, 1, False, True))
    args.append((100 * 1000 * 1000, 10 * 1000, dist, 2, False, True))

# args.append((100 * 1000 * 1000, 10 * 1000, "uniform5", 1, False, True))
# args.append((100 * 1000 * 1000, 10 * 1000, "uniform5", 2, False, True))
# args.append((100 * 1000 * 1000, 10 * 1000, "normal5", 1, False, True))
# args.append((100 * 1000 * 1000, 10 * 1000, "normal5", 2, False, True))
# args.append((100 * 1000 * 1000, 10 * 1000, "powerlaw5", 1, False, True))
# args.append((100 * 1000 * 1000, 10 * 1000, "powerlaw5", 2, False, True))

#  args.append((1000 * 1000,  100 * 1000, dist, 2, False, True))
#  max_var_args.append(
#  (100 * 1000 * 1000, 10 * 1000 * 1000, dist, False, True))
# for rel in ["powerlaw2"]:
#     where_args.append(
#         (100 * 1000 * 1000, 10 * 1000 * 1000, dist, rel, 100, 1, False, True)
#     )
#  dg.create_table_data_for_where(10 * 1000 * 1000, 1 * 1000 * 1000, dist,
#  rel, 10, 1, False, True)
#  dg.create_table_data_for_where(10 * 1000 * 1000, 10 * 1000 * 1000,
#  dist, rel, 10, 1, False, True)

#  for arg in args:
#  pool.apply_async(dg.create_table_data, arg)
results = pool.starmap(dg.create_table_data, args)
results = pool.starmap(dg.create_table_data_for_where, where_args)
results = pool.starmap(dg.create_max_var_table_data, max_var_args)

#  for i in [1, 2]:
#  dg.create_table(1000 * 1000 * 1000, 250 * 1000 * 1000, dist, i, True)
#
#  for i in [1, 2]:
#  dg.create_table(1000 * 1000 * 1000, 100 * 1000 * 1000, dist, i, True)

pool.close()
pool.join()
