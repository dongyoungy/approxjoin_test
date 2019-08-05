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


num_proc = 20

pool = mp.Pool(processes=num_proc, maxtasksperchild=10)
num_instacart_samples = 500
num_synthetic_samples = 100
overwrite = False
dec_args = []
args = []
results = []

impala_host = "cp-14"
impala_port = 21050

# for stratified samples for synthetic (preset)
for leftDist in ["normal_powerlaw2_1"]:
    for rightDist in ["normal_2"]:
        for i in range(1, 100 + 1):
            args.append(
                (
                    impala_host,
                    impala_port,
                    "synthetic_10m",
                    leftDist,
                    "col1",
                    "col3",
                    "synthetic_10m",
                    rightDist,
                    "col1",
                    "synthetic_10m_preset_strat5",
                    100000,
                    0.01,
                    num_synthetic_samples,
                    i,
                    overwrite,
                )
            )

# create samples
for arg in args:
    results.append(
        pool.apply_async(
            sg.create_preset_stratified_sample_pair_from_impala_individual,
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
