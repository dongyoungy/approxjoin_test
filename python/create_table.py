import data_gen as dg
import multiprocessing as mp

args = []
num_proc = 10
pool = mp.Pool(processes=num_proc, maxtasksperchild=10)

for dist in ['uniform', 'normal', 'powerlaw']:
    for rel in ['normal', 'powerlaw']:
        args.append((10 * 1000 * 1000, 10 * 1000 * 1000, dist, rel, 10, 1,
                     False, True))
        args.append(
            (10 * 1000 * 1000, 1 * 1000 * 1000, dist, rel, 10, 1, False, True))
        #  dg.create_table_data_for_where(10 * 1000 * 1000, 1 * 1000 * 1000, dist,
        #  rel, 10, 1, False, True)
        #  dg.create_table_data_for_where(10 * 1000 * 1000, 10 * 1000 * 1000,
        #  dist, rel, 10, 1, False, True)

results = pool.starmap(dg.create_table_data_for_where, args)

#  for i in [1, 2]:
#  dg.create_table(1000 * 1000 * 1000, 250 * 1000 * 1000, dist, i, True)
#
#  for i in [1, 2]:
#  dg.create_table(1000 * 1000 * 1000, 100 * 1000 * 1000, dist, i, True)
