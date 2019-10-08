import timeit
import sample_gen as sg
import data_gen as dg
import multiprocessing as mp
import calculate_agg as cal
import sys
from itertools import product

from email.mime.text import MIMEText
from subprocess import Popen, PIPE


def callback_error(result):
    print("Error: " + str(result.__cause__), file=sys.stderr, flush=True)
    msg = MIMEText("N/A")
    msg["From"] = "dyoon@umich.edu"
    msg["To"] = "dyoon@umich.edu"
    msg["Subject"] = "Run Failed."
    p = Popen(["/usr/sbin/sendmail", "-t", "-oi"], stdin=PIPE)
    p.communicate(msg.as_bytes())
    sys.exit("Terminate due to subprocess failure")


def callback_success(result):
    print("Success")


num_proc = 24

pool = mp.Pool(processes=num_proc, maxtasksperchild=10)
num_instacart_samples = 500
num_synthetic_samples = 100
overwrite = False
dec_args = []
old_args = []
new_args = []
results = []

impala_host = "cp-18"
impala_port = 21050

e1 = 0.03
e2 = 0.01

# for stratified samples for synthetic (old)
for leftDist in ["normal_powerlaw2_1"]:
    for rightDist in ["normal_2"]:
        for agg in ["count", "sum", "avg"]:
            for key_t in [1000, 10000, 100000, 1000000]:
                for row_t in [10, 100]:
                    old_args.append(
                        (
                            impala_host,
                            impala_port,
                            "synthetic_10m",
                            leftDist,
                            "col1",
                            "col2",
                            "col3",
                            "synthetic_10m",
                            rightDist,
                            "col1",
                            "old_st_test2",
                            agg,
                            key_t,
                            row_t,
                            e1,
                            e2,
                            num_synthetic_samples,
                            overwrite,
                        )
                    )

# for stratified samples for synthetic (new)
# for leftDist in ["normal_powerlaw2_1"]:
#     for rightDist in ["normal_2"]:
#         for agg in ["count", "sum", "avg"]:
#             # for key_t in [1000, 10000, 100000, 1000000]:
#             for key_t in [10000]:
#                 new_args.append(
#                     (
#                         impala_host,
#                         impala_port,
#                         "synthetic_10m",
#                         leftDist,
#                         "col1",
#                         "col2",
#                         "col3",
#                         "synthetic_10m",
#                         rightDist,
#                         "col1",
#                         "new_st_test2",
#                         agg,
#                         key_t,
#                         100000,
#                         e1,
#                         e2,
#                         num_synthetic_samples,
#                         overwrite,
#                     )
#                 )

# create stratified samples
for arg in old_args:
    results.append(
        pool.apply_async(
            sg.create_cent_stratified_sample_pair_from_impala,
            arg,
            callback=callback_success,
            error_callback=callback_error,
        )
    )

for arg in new_args:
    results.append(
        pool.apply_async(
            sg.create_cent_stratified_sample_pair_from_impala_new,
            arg,
            callback=callback_success,
            error_callback=callback_error,
        )
    )

pool.close()
pool.join()

msg = MIMEText("N/A")
msg["From"] = "dyoon@umich.edu"
msg["To"] = "dyoon@umich.edu"
msg["Subject"] = "Run Successful"
p = Popen(["/usr/sbin/sendmail", "-t", "-oi"], stdin=PIPE)
p.communicate(msg.as_bytes())
