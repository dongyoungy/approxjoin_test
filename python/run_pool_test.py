import sample_gen as sg
import multiprocessing as mp

num_proc = 4
pool = mp.Pool(processes=num_proc, maxtasksperchild=10)
host = "cp-10"
port = 21050

old_args = []
new_args = []

old_args.append(
    (
        "cp-18",
        21050,
        "synthetic_10m",
        "normal_powerlaw2_1",
        "col1",
        "col2",
        "col3",
        "synthetic_10m",
        "normal_2",
        "col1",
        "old_st_test2",
        "count",
        10000,
        10,
        0.03,
        0.01,
        1,
        False,
    )
)

new_args.append(
    (
        "cp-18",
        21050,
        "synthetic_10m",
        "normal_powerlaw2_1",
        "col1",
        "col2",
        "col3",
        "synthetic_10m",
        "normal_2",
        "col1",
        "new_st_test2",
        "count",
        10000,
        100000,
        0.03,
        0.01,
        1,
        False,
    )
)

# for i in range(1, 100):
#     args.append(
#         (host, port, "synthetic_10m", "normal_powerlaw2_1", "col1", "col2", "col3", i)
#     )

for arg in old_args:
    pool.apply_async(sg.create_cent_stratified_sample_pair_from_impala, arg)

for arg in new_args:
    pool.apply_async(sg.create_cent_stratified_sample_pair_from_impala_new, arg)

pool.close()
pool.join()
