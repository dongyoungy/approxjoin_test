import timeit
import sample_gen as sg

setup = "import sample_gen as sg"
sg.reset_schema()

for dist in ['uniform']:
    for type in ['count', 'sum']:
        for isCentralized in [True, False]:
            s = "sg.create_sample(1000*1000*1000, 100*1000*1000, '{0}', '{0}', '{1}', {2})".format(
            dist, type, isCentralized)
            #  s = "sg.create_sample(100000, 10000, '{0}', '{0}', '{1}', {2})".format(
                #  dist, type, isCentralized)
            d = ''
            if isCentralized:
                d = 'cent'
            else:
                d = 'dec'
            t = timeit.timeit(stmt=s, setup=setup, number=1)
            print("[{0}, {1}, {2}] = {3} s".format(dist, type, d, t))
