import os
import time
import io
import math
import numpy as np
import scipy.stats as ss


def create_table_data(num_rows, num_keys, type):
    remaining = num_rows
    # write 1M records at a time
    default_batch_size = 1000000
    # seed the rng
    np.random.seed(int(time.time()))

    while remaining > 0:
        buf = io.StringIO
        current_batch_size = default_batch_size

        if remaining < default_batch_size:
            current_batch_size = remaining

        if type == 'uniform':
            keys = np.random.randint(1, num_keys + 1, current_batch_size)

        elif type == 'normal':
            # from: https://stackoverflow.com/questions/37411633/how-to-generate-a-random-normal-distribution-of-integers
            r = num_keys / 2
            scale = num_keys / 5
            keys = ss.truncnorm(
                a=(-r + 1) / scale, b=r / scale, scale=scale).rvs(num_rows)
            keys = keys + r
            keys = keys.round().astype(int)

        elif type == 'powerlaw':
            alpha = -2.5
            minv = 1
            maxv = num_keys
            rand_keys = np.array(np.random.random(size=current_batch_size))
            keys = ((maxv**(alpha + 1) - minv**
                     (alpha + 1)) * rand_keys + minv**
                    (alpha + 1))**(1 / (alpha + 1))
            keys = [math.floor(k) for k in keys]

        else:
            print("Unsupported type: {0}".format(type))
            return

        for k in keys:
            print(k)

        remaining -= current_batch_size

    return keys
