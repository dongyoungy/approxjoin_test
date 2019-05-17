import numpy as np
from collections import defaultdict


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
            for pr in prob:
                p = pr[0]
                q = pr[1]
                print("T1 = {}, Agg = {}, p = {}, q = {}".format(
                    t1, agg, p, q))
                for t2 in t2s:
                    print("{:.3f}".format(new_results[t1][t2][(agg, p,
                                                                       q)]))

    return new_results
