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


num_proc = 3

pool = mp.Pool(processes=num_proc, maxtasksperchild=10)
num_instacart_samples = 500
num_synthetic_samples = 100
overwrite = False
dec_args = []
args = []
results = []

impala_host = "cp-18"
impala_port = 21050

# for stratified samples for synthetic
for leftDist in ["normal_powerlaw_1"]:
    for rightDist in ["normal_2"]:
        for agg in ["count", "sum"]:
            args.append(
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
                    "synthetic_10m_strat6",
                    agg,
                    1000000,
                    100000,
                    num_synthetic_samples,
                    overwrite,
                )
            )

# for all samples for instacart
# for leftDist in ["orders"]:
#     for rightDist in ["order_products"]:
#         args.append(
#             (
#                 impala_host,
#                 impala_port,
#                 "instacart",
#                 leftDist,
#                 "order_id",
#                 "days_since_prior",
#                 "order_dow",
#                 "instacart",
#                 rightDist,
#                 "order_id",
#                 "instacart_all2",
#                 num_synthetic_samples,
#                 overwrite,
#             )
#         )

# create samples
for arg in args:
    results.append(
        pool.apply_async(
            sg.create_cent_stratified_sample_pair_from_impala,
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
