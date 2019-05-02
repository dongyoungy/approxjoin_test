import timeit
import sample_gen as sg
import data_gen as dg
import multiprocessing as mp
import calculate_agg as cal

num_proc = 10

pool = mp.Pool(num_proc)


dists = []

# for centralized setting
dists.append(('uniform', 'uniform'))
dists.append(('uniform', 'normal'))
dists.append(('uniform', 'powerlaw'))
dists.append(('normal', 'normal'))
dists.append(('normal', 'powerlaw'))
dists.append(('powerlaw', 'powerlaw'))

aggs = ['count', 'sum', 'avg']
is_centralized_list = [True]

num_samples = 100

args = []

for num_row in [10 * 1000 * 1000]:
    for num_key in [10 * 1000 * 1000]:
        for dist in dists:
            for agg in aggs:
                for is_centralized in is_centralized_list:
                    for s in range(1, num_samples + 1):
                        args.append((num_row, num_key, dist[0], dist[1], agg,
                                     s, is_centralized))

results = pool.starmap(cal.estimate_agg, args)

pool.close()
pool.join()
