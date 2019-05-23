from collections import defaultdict
from threading import RLock

import Cond
import numpy as np
import pandas as pd
from cachetools import LRUCache, cached

from estimate_result import EstimateResult

actual_cache = LRUCache(maxsize=4 * 1024)
estimate_cache = LRUCache(maxsize=16 * 1024)
lock = RLock()
data_path = '/home/dyoon/work/approxjoin_data/'


def get_group_by_average(table):
    d = defaultdict(list)
    for row in table:
        d[row[0]].append(row[1])

    for k in d:
        v = d[k]
        d[k] = sum(v) / len(v)

    return d


@cached(actual_cache, lock=lock)
def calculate_actual(num_rows,
                     num_keys,
                     leftDist,
                     rightDist,
                     aggFunc,
                     cond=None):
    raw_data_path = data_path + 'raw_data'
    T1_name = raw_data_path + "/t_{0}n_{1}k_{2}_{3}.csv".format(
        num_rows, num_keys, leftDist, 1)
    T2_name = raw_data_path + "/t_{0}n_{1}k_{2}_{3}.csv".format(
        num_rows, num_keys, rightDist, 2)

    # read table files
    T1_df = pd.read_csv(T1_name, sep='|', header=None, usecols=[0, 1, 2])
    # drop dummy col
    #  T1_df = T1_df.drop(columns=[3])
    T1 = T1_df.values

    if cond is not None:
        T1 = cond.apply_array(T1)
    T2_df = pd.read_csv(T2_name, sep='|', header=None, usecols=[0, 1, 2])
    #  T2_df = T2_df.drop(columns=[3])
    T2 = T2_df.values

    all_keys = np.arange(1, num_keys + 1)

    T1_freq = np.zeros((num_keys, 2))
    T2_freq = np.zeros((num_keys, 2))

    T1_freq[:, 0] = all_keys
    T2_freq[:, 0] = all_keys

    T1_counts = np.array(np.unique(T1[:, 0], return_counts=True)).T
    T1_freq[T1_counts[:, 0].astype(int) - 1, 1] = T1_counts[:, 1]

    T2_counts = np.array(np.unique(T2[:, 0], return_counts=True)).T
    T2_freq[T2_counts[:, 0].astype(int) - 1, 1] = T2_counts[:, 1]

    actual = None
    if aggFunc == 'count':
        actual = sum(T1_freq[:, 1] * T2_freq[:, 1])
    elif aggFunc == 'sum':
        mu = np.zeros((num_keys, 1))

        d = get_group_by_average(T1)
        for k in d:
            mu[int(k - 1), 0] = d[k]

        actual = sum(mu[:, 0] * T1_freq[:, 1] * T2_freq[:, 1])
    elif aggFunc == 'avg':
        mu = np.zeros((num_keys, 1))

        d = get_group_by_average(T1)
        for k in d:
            mu[int(k - 1), 0] = d[k]

        denom = sum(T1_freq[:, 1] * T2_freq[:, 1])

        actual = sum(mu[:, 0] * T1_freq[:, 1] *
                     T2_freq[:, 1]) / denom if denom != 0 else 0
    else:
        print("Unsupported func: {}".format(aggFunc))

    return actual


@cached(actual_cache, lock=lock)
def calculate_actual_count_with_cond(num_rows,
                                     num_keys,
                                     leftDist,
                                     rightDist,
                                     rel_type,
                                     cond=None):
    raw_data_path = data_path + 'raw_data'
    T1_name = raw_data_path + "/t_{0}n_{1}k_{2}_{3}_{4}.csv".format(
        num_rows, num_keys, leftDist, rel_type, 1)
    T2_name = raw_data_path + "/t_{0}n_{1}k_{2}_{3}.csv".format(
        num_rows, num_keys, rightDist, 2)

    # read table files
    T1_df = pd.read_csv(T1_name, sep='|', header=None, usecols=[0, 1, 2])
    # drop dummy col
    #  T1_df = T1_df.drop(columns=[3])
    T1 = T1_df.values

    if cond is not None:
        T1 = cond.apply_array(T1)
    T2_df = pd.read_csv(T2_name, sep='|', header=None, usecols=[0, 1, 2])
    #  T2_df = T2_df.drop(columns=[3])
    T2 = T2_df.values

    all_keys = np.arange(1, num_keys + 1)

    T1_freq = np.zeros((num_keys, 2))
    T2_freq = np.zeros((num_keys, 2))

    T1_freq[:, 0] = all_keys
    T2_freq[:, 0] = all_keys

    T1_counts = np.array(np.unique(T1[:, 0], return_counts=True)).T
    T1_freq[T1_counts[:, 0].astype(int) - 1, 1] = T1_counts[:, 1]

    T2_counts = np.array(np.unique(T2[:, 0], return_counts=True)).T
    T2_freq[T2_counts[:, 0].astype(int) - 1, 1] = T2_counts[:, 1]

    actual = sum(T1_freq[:, 1] * T2_freq[:, 1])

    return actual


@cached(estimate_cache, lock=lock)
def estimate_count_with_cond(num_rows,
                             num_keys,
                             leftDist,
                             rightDist,
                             var_dist,
                             rel_type,
                             sample_idx,
                             cond=None,
                             isCentralized=True):
    dir = data_path + 'our_samples/with_cond/'
    d = ''
    if isCentralized:
        d = 'centralized'
    else:
        d = 'decentralized'

    sample_dir = dir + d + '/'
    dir = "{}/{}n_{}k/{}_{}/{}_{}".format(sample_dir, num_rows, num_keys,
                                          leftDist, rightDist, var_dist,
                                          rel_type)

    S1_name = "{}/s1_{}.npy".format(dir, sample_idx)
    S2_name = "{}/s2_{}.npy".format(dir, sample_idx)

    S1_data = np.load(S1_name)
    S2_data = np.load(S2_name)

    S1_data = S1_data[()]
    S2_data = S2_data[()]

    S1 = S1_data['sample']
    p1 = S1_data['p']
    q1 = S1_data['q']

    S2 = S2_data['sample']
    p2 = S2_data['p']
    q2 = S2_data['q']

    if cond is not None:
        if not isinstance(cond, Cond.Cond):
            print("cond must be a Cond object.")
            return
        S1 = cond.apply_array(S1)

    keys = np.arange(1, num_keys + 1)

    S1_freq = np.zeros((num_keys, 2))
    S2_freq = np.zeros((num_keys, 2))

    S1_freq[:, 0] = keys
    S2_freq[:, 0] = keys

    S1_counts = np.array(np.unique(S1[:, 0], return_counts=True)).T
    S1_freq[S1_counts[:, 0].astype(int) - 1, 1] = S1_counts[:, 1]

    S2_counts = np.array(np.unique(S2[:, 0], return_counts=True)).T
    S2_freq[S2_counts[:, 0].astype(int) - 1, 1] = S2_counts[:, 1]

    actual = calculate_actual_count_with_cond(num_rows, num_keys, leftDist,
                                              rightDist, rel_type, cond)
    if actual is None:
        print("Could not calculate actual agg.")
        return

    estimate = sum(S1_freq[:, 1] * S2_freq[:, 1])
    p = min([p1, p2])
    estimate = estimate * (1 / (p * q1 * q2))

    p = p1
    q = q1

    # duplicate info just in case
    result = EstimateResult()
    result.actual = actual
    result.estimate = estimate
    result.p = p
    result.q = q
    result.num_rows = num_rows
    result.num_keys = num_keys
    result.left_dist = leftDist
    result.right_dist = rightDist
    result.agg_func = 'count'
    result.var_dist = var_dist
    result.rel_type = rel_type
    result.cond_value = cond.value
    result.cond = cond

    return (num_rows, num_keys, leftDist, rightDist, 'count', var_dist,
            rel_type, cond.value, sample_idx, result)


@cached(estimate_cache, lock=lock)
def estimate_agg(num_rows, num_keys, leftDist, rightDist, aggFunc, sample_idx,
                 isCentralized):
    dir = data_path + 'our_samples/'
    d = ''
    if isCentralized:
        d = 'centralized'
    else:
        d = 'decentralized'

    sample_dir = dir + d + '/'
    dir = "{}/{}n_{}k/{}_{}/{}".format(sample_dir, num_rows, num_keys,
                                       leftDist, rightDist, aggFunc)

    S1_name = "{}/s1_{}.npy".format(dir, sample_idx)
    S2_name = "{}/s2_{}.npy".format(dir, sample_idx)

    S1_data = np.load(S1_name)
    S2_data = np.load(S2_name)

    S1_data = S1_data[()]
    S2_data = S2_data[()]

    S1 = S1_data['sample']
    p1 = S1_data['p']
    q1 = S1_data['q']

    S2 = S2_data['sample']
    p2 = S2_data['p']
    q2 = S2_data['q']

    keys = np.arange(1, num_keys + 1)

    S1_freq = np.zeros((num_keys, 2))
    S2_freq = np.zeros((num_keys, 2))

    S1_freq[:, 0] = keys
    S2_freq[:, 0] = keys

    S1_counts = np.array(np.unique(S1[:, 0], return_counts=True)).T
    S1_freq[S1_counts[:, 0].astype(int) - 1, 1] = S1_counts[:, 1]

    S2_counts = np.array(np.unique(S2[:, 0], return_counts=True)).T
    S2_freq[S2_counts[:, 0].astype(int) - 1, 1] = S2_counts[:, 1]

    actual = calculate_actual(num_rows, num_keys, leftDist, rightDist, aggFunc)
    if actual is None:
        print("Could not calculate actual agg.")
        return

    if aggFunc == 'count':
        estimate = sum(S1_freq[:, 1] * S2_freq[:, 1])
        p = min([p1, p2])
        estimate = estimate * (1 / (p * q1 * q2))
    elif aggFunc == 'sum':
        mu = np.zeros((num_keys, 1))
        d = get_group_by_average(S1)
        for k in d:
            mu[int(k - 1), 0] = d[k]

        # MATLAB implementation
        #  estimate = sum(mu(:,2) .* S1freq(:,2) .* S2freq(:,2));
        #  p = min(p1, p2);
        #  estimate = estimate * (1 / (p * q1 * q2));
        estimate = sum(mu[:, 0] * S1_freq[:, 1] * S2_freq[:, 1])
        p = min([p1, p2])
        estimate = estimate * (1 / (p * q1 * q2))
    elif aggFunc == 'avg':
        mu = np.zeros((num_keys, 1))
        d = get_group_by_average(S1)
        for k in d:
            mu[int(k - 1), 0] = d[k]

        # get mean and var
        #  gr = S1_df.groupby(0)
        #  keys = np.array(list(gr.groups.keys()))
        #  means = np.array(gr[1].mean().values)
        #  mu[keys - 1, 1] = means

        estimate_count = sum(S1_freq[:, 1] * S2_freq[:, 1])
        estimate_sum = sum(mu[:, 0] * S1_freq[:, 1] * S2_freq[:, 1])
        p = min([p1, p2])
        estimate_sum = estimate_sum * (1 / (p * q1 * q2))
        estimate_count = estimate_count * (1 / (p * q1 * q2))
        estimate = estimate_sum / estimate_count if estimate_count != 0 else 0
    else:
        print("Unsupported function: {}".format(aggFunc))
        return

    p = p1
    q = q1

    # duplicate info just in case
    result = EstimateResult()
    result.actual = actual
    result.estimate = estimate
    result.p = p
    result.q = q
    result.num_rows = num_rows
    result.num_keys = num_keys
    result.left_dist = leftDist
    result.right_dist = rightDist
    result.agg_func = aggFunc

    return (num_rows, num_keys, leftDist, rightDist, aggFunc, sample_idx,
            result)


@cached(estimate_cache, lock=lock)
def estimate_preset_agg(num_rows, num_keys, leftDist, rightDist, aggFunc, p, q,
                        sample_idx):
    sample_dir = data_path + 'preset_samples'
    dir = "{}/{}n_{}k/{}_{}/{:.3f}_{:.3f}/".format(sample_dir, num_rows,
                                                   num_keys, leftDist,
                                                   rightDist, p, q)

    S1_name = "{}/s1_{}.npy".format(dir, sample_idx)
    S2_name = "{}/s2_{}.npy".format(dir, sample_idx)

    S1_data = np.load(S1_name)
    S2_data = np.load(S2_name)

    S1_data = S1_data[()]
    S2_data = S2_data[()]

    S1 = S1_data['sample']
    p1 = S1_data['p']
    q1 = S1_data['q']

    S2 = S2_data['sample']
    p2 = S2_data['p']
    q2 = S2_data['q']

    keys = np.arange(1, num_keys + 1)

    S1_freq = np.zeros((num_keys, 2))
    S2_freq = np.zeros((num_keys, 2))

    S1_freq[:, 0] = keys
    S2_freq[:, 0] = keys

    S1_counts = np.array(np.unique(S1[:, 0], return_counts=True)).T
    S1_freq[S1_counts[:, 0].astype(int) - 1, 1] = S1_counts[:, 1]

    S2_counts = np.array(np.unique(S2[:, 0], return_counts=True)).T
    S2_freq[S2_counts[:, 0].astype(int) - 1, 1] = S2_counts[:, 1]

    actual = calculate_actual(num_rows, num_keys, leftDist, rightDist, aggFunc)
    if actual is None:
        print("Could not calculate actual agg.")
        return

    if aggFunc == 'count':
        estimate = sum(S1_freq[:, 1] * S2_freq[:, 1])
        p = min([p1, p2])
        estimate = estimate * (1 / (p * q1 * q2))
    elif aggFunc == 'sum':
        mu = np.zeros((num_keys, 1))

        d = get_group_by_average(S1)
        for k in d:
            mu[int(k - 1), 0] = d[k]

        # MATLAB implementation
        #  estimate = sum(mu(:,2) .* S1freq(:,2) .* S2freq(:,2));
        #  p = min(p1, p2);
        #  estimate = estimate * (1 / (p * q1 * q2));
        estimate = sum(mu[:, 0] * S1_freq[:, 1] * S2_freq[:, 1])
        p = min([p1, p2])
        estimate = estimate * (1 / (p * q1 * q2))
    elif aggFunc == 'avg':
        mu = np.zeros((num_keys, 1))

        d = get_group_by_average(S1)
        for k in d:
            mu[int(k - 1), 0] = d[k]

        estimate_count = sum(S1_freq[:, 1] * S2_freq[:, 1])
        estimate_sum = sum(mu[:, 0] * S1_freq[:, 1] * S2_freq[:, 1])
        p = min([p1, p2])
        estimate_sum = estimate_sum * (1 / (p * q1 * q2))
        estimate_count = estimate_count * (1 / (p * q1 * q2))
        estimate = estimate_sum / estimate_count if estimate_count != 0 else 0
    else:
        print("Unsupported function: {}".format(aggFunc))
        return

    result = EstimateResult()
    result.actual = actual
    result.estimate = estimate
    result.p = p
    result.q = q
    result.num_rows = num_rows
    result.num_keys = num_keys
    result.left_dist = leftDist
    result.right_dist = rightDist
    result.agg_func = aggFunc

    return (num_rows, num_keys, leftDist, rightDist, aggFunc, p, q, sample_idx,
            result)


@cached(estimate_cache, lock=lock)
def estimate_preset_count_with_cond(num_rows,
                                    num_keys,
                                    leftDist,
                                    rightDist,
                                    p,
                                    q,
                                    rel_type,
                                    sample_idx,
                                    cond=None):
    sample_dir = data_path + 'preset_samples/with_cond'
    dir = "{}/{}n_{}k/{}_{}/{}/{:.3f}_{:.3f}/".format(sample_dir, num_rows,
                                                      num_keys, leftDist,
                                                      rightDist, rel_type, p,
                                                      q)

    S1_name = "{}/s1_{}.npy".format(dir, sample_idx)
    S2_name = "{}/s2_{}.npy".format(dir, sample_idx)

    S1_data = np.load(S1_name)
    S2_data = np.load(S2_name)

    S1_data = S1_data[()]
    S2_data = S2_data[()]

    S1 = S1_data['sample']
    p1 = S1_data['p']
    q1 = S1_data['q']

    S2 = S2_data['sample']
    p2 = S2_data['p']
    q2 = S2_data['q']

    if cond is not None:
        if not isinstance(cond, Cond.Cond):
            print("cond must be a Cond object.")
            return
        S1 = cond.apply_array(S1)

    keys = np.arange(1, num_keys + 1)

    S1_freq = np.zeros((num_keys, 2))
    S2_freq = np.zeros((num_keys, 2))

    S1_freq[:, 0] = keys
    S2_freq[:, 0] = keys

    S1_counts = np.array(np.unique(S1[:, 0], return_counts=True)).T
    S1_freq[S1_counts[:, 0].astype(int) - 1, 1] = S1_counts[:, 1]

    S2_counts = np.array(np.unique(S2[:, 0], return_counts=True)).T
    S2_freq[S2_counts[:, 0].astype(int) - 1, 1] = S2_counts[:, 1]

    actual = calculate_actual_count_with_cond(num_rows, num_keys, leftDist,
                                        rightDist, rel_type, cond)
    if actual is None:
        print("Could not calculate actual agg.")
        return

    estimate = sum(S1_freq[:, 1] * S2_freq[:, 1])
    p = min([p1, p2])
    estimate = estimate * (1 / (p * q1 * q2))

    result = EstimateResult()
    result.actual = actual
    result.estimate = estimate
    result.p = p
    result.q = q
    result.num_rows = num_rows
    result.num_keys = num_keys
    result.left_dist = leftDist
    result.right_dist = rightDist
    result.agg_func = 'count'
    result.rel_type = rel_type
    result.cond_value = cond.value
    result.cond = cond

    return (num_rows, num_keys, leftDist, rightDist, 'count', p, q, rel_type,
            cond.value, sample_idx, result)
