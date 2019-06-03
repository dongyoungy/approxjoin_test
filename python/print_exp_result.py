import numpy as np
import pandas as pd
from collections import defaultdict
from cachetools import LRUCache, cached

dist_pairs = []
dist_pairs.append(('uniform', 'uniform'))
dist_pairs.append(('uniform', 'normal'))
dist_pairs.append(('uniform', 'powerlaw'))
dist_pairs.append(('normal', 'normal'))
dist_pairs.append(('normal', 'powerlaw'))
var_dists = ['uniform', 'normal', 'powerlaw']
cond_dists = ['uniform', 'identical']

probs = []
probs.append((0.01, 1))
probs.append((0.015, 0.666))
probs.append((0.03, 0.333))
probs.append((0.333, 0.03))
probs.append((0.666, 0.015))
probs.append((1, 0.01))

raw_data_path = '/home/dyoon/work/approxjoin_data/raw_data'

dist_cache = LRUCache(maxsize=16 * 1024)


@cached(dist_cache)
def get_dist_of_X(num_rows, num_keys, dist, rel_type):
    T1_name = raw_data_path + "/t_{0}n_{1}k_{2}_{3}_{4}.csv".format(
        num_rows, num_keys, dist, rel_type, 1)
    # read table files
    T1_df = pd.read_csv(T1_name, sep='|', header=None, usecols=[0, 1, 2])
    T1 = T1_df.values
    T1 = T1.astype(int)
    gr = T1_df.groupby([2])
    counts = np.array(gr.count().values)
    return counts[:, 0]


def print_our_results_with_cond(results, actual_cond_dist='uniform'):
    results = results[()]
    new_results = defaultdict(dict)

    if actual_cond_dist == 'identical':
        pass

    for k in results.keys():
        if k == 0:
            continue
        result = results[k]

        # we need (left, right, var_dist, rel_type, cond_val) as key?
        new_key = (k[2], k[3], k[5], k[6], k[7])
        if new_key not in new_results:
            new_results[new_key] = []

        new_results[new_key].append(result[0])

    for cond_dist in cond_dists:
        for var_dist in var_dists:
            for d in dist_pairs:
                vars = []
                p = 0
                q = 0
                if actual_cond_dist == 'identical':
                    x_dists = get_dist_of_X(10 * 1000 * 1000, 1000 * 1000,
                                            d[0], var_dist)
                    x_total = np.sum(x_dists)
                for c in range(0, 10):
                    res = new_results[(d[0], d[1], cond_dist, var_dist, c)]
                    estimates = []
                    for row in res:
                        estimates.append(row.estimate)
                        p = row.p
                        q = row.q
                    if actual_cond_dist == 'uniform':
                        vars.append(np.var(estimates))
                    elif actual_cond_dist == 'identical':
                        vars.append(np.var(estimates) * (x_dists[c] / x_total))
                # if actual dist of X is uniform
                if actual_cond_dist == 'uniform':
                    print("{}".format(np.average(vars)), end=",")
                elif actual_cond_dist == 'identical':
                    print("{}".format(np.sum(vars)), end=",")
            print()
        print()
        print()


def print_preset_results_with_cond(results, actual_cond_dist='uniform'):
    results = results[()]
    new_results = defaultdict(dict)

    for k in results.keys():
        if k == 0:
            continue
        result = results[k]

        # we need (left, right, p, q, rel_type, cond_val) as key?
        new_key = (k[2], k[3], k[5], k[6], k[7], k[8])
        if new_key not in new_results:
            new_results[new_key] = []

        new_results[new_key].append(result[0])

    for v in var_dists:
        if actual_cond_dist == 'identical':
            x_dists = get_dist_of_X(10 * 1000 * 1000, 1000 * 1000,
                                    'uniform', v)
            x_total = np.sum(x_dists)
        for p in probs:
            vars = []
            for c in range(0, 10):
                res = new_results[('normal', 'normal', p[0], p[1], v, c)]
                estimates = []
                for row in res:
                    estimates.append(row.estimate)
                if actual_cond_dist == 'uniform':
                    vars.append(np.var(estimates))
                elif actual_cond_dist == 'identical':
                    vars.append(np.var(estimates) * (x_dists[c] / x_total))
            # if actual dist of X is uniform
            if actual_cond_dist == 'uniform':
                print("{}".format(np.average(vars)), end=",")
            elif actual_cond_dist == 'identical':
                print("{}".format(np.sum(vars)), end=",")
        print()


def print_dec_result(results):
    results = results[()]  # strip out of the array
    new_results = defaultdict(dict)
    for k in results.keys():
        if k == 0:
            continue
        result = results[k]
        num_result = len(result)
        estimates = []
        for i in range(0, num_result):
            p = result[i].p
            q = result[i].q
            estimates.append(result[i].estimate)

        v = np.var(estimates)

        t1 = k[2]
        t2 = k[3]
        agg = k[4]

        if t1 not in new_results or t2 not in new_results[t1]:
            new_results[t1][t2] = {}
        new_results[t1][t2][agg] = (p, q, v)
        print("{} (p = {:.3f}, q = {:.3f}): {:.3f}".format(k, p, q, v))

    t1s = ['uniform', 'normal', 'powerlaw']
    aggs = ['count', 'sum']
    for agg in aggs:
        for t1 in t1s:
            print("T1 = {}, Agg = {}".format(t1, agg))
            t2s = [
                'uniform', 'normal', 'normal1', 'normal2', 'powerlaw',
                'powerlaw1', 'powerlaw2'
            ]
            t2s.append(t1 + '_max_var')
            for t2 in t2s:
                print("{:.3f},{:.3f},{:.3f}".format(
                    new_results[t1][t2][agg][0], new_results[t1][t2][agg][1],
                    new_results[t1][t2][agg][2]))

    return new_results


def print_preset_result(results):

    prob = []
    prob.append((0.01, 1))
    prob.append((0.015, 0.666))
    prob.append((0.03, 0.333))
    prob.append((0.333, 0.03))
    prob.append((0.666, 0.015))
    prob.append((1, 0.01))

    results = results[()]  # strip out of the array
    new_results = defaultdict(dict)
    for k in results.keys():
        if k == 0:
            continue
        result = results[k]
        num_result = len(result)
        r = defaultdict(list)
        for i in range(0, num_result):
            p = result[i].p
            q = result[i].q
            r[(p, q)].append(result[i].estimate)

        vars = {}
        for k2 in r.keys():
            vars[k2] = np.var(r[k2])

        t1 = k[2]
        t2 = k[3]
        agg = k[4]

        if t1 not in new_results or t2 not in new_results[t1]:
            new_results[t1][t2] = {}

        for pq in vars.keys():
            new_results[t1][t2][(agg, pq[0], pq[1])] = vars[pq]

    t1s = ['uniform', 'normal', 'powerlaw']
    aggs = ['count', 'sum']
    for agg in aggs:
        for t1 in t1s:
            print("T1 = {}, Agg = {}".format(t1, agg))
            t2s = [
                'uniform', 'normal', 'normal1', 'normal2', 'powerlaw',
                'powerlaw1', 'powerlaw2'
            ]
            t2s.append(t1 + '_max_var')

            num_prob = len(prob)
            num_t2 = len(t2s)

            arr = np.ndarray((num_t2, num_prob))

            for pr in prob:
                p = pr[0]
                q = pr[1]
                print("T1 = {}, Agg = {}, p = {}, q = {}".format(
                    t1, agg, p, q))
                for t2 in t2s:
                    val = new_results[t1][t2][(agg, p, q)]
                    arr[t2s.index(t2)][prob.index(pr)] = val
                    print("{:.3f}".format(new_results[t1][t2][(agg, p, q)]))

            for i in range(0, num_t2):
                for j in range(0, num_prob):
                    print(arr[i][j], end=",")
                print()

    return new_results
