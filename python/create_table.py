import data_gen as dg

for dist in ['uniform']:
    dg.create_table(10 * 1000 * 1000, 10 * 1000 * 1000, dist, 1, True)
    dg.create_table(10 * 1000 * 1000, 10 * 1000 * 1000, dist, 2, True)
    dg.create_table(100 * 1000 * 1000, 10 * 1000 * 1000, dist, 2, True)
    #  for i in [1, 2]:
        #  dg.create_table(1000 * 1000 * 1000, 250 * 1000 * 1000, dist, i, True)
#
    #  for i in [1, 2]:
        #  dg.create_table(1000 * 1000 * 1000, 100 * 1000 * 1000, dist, i, True)
