import data_gen as dg

for dist in ['uniform', 'normal', 'powerlaw']:
    for rel in ['uniform', 'positive', 'negative']:
        dg.create_table_data_for_where(10 * 1000 * 1000, 1 * 1000 * 1000, dist,
                                       rel, 1, False, True)
        dg.create_table_data_for_where(10 * 1000 * 1000, 10 * 1000 * 1000,
                                       dist, rel, 1, False, True)
    #  for i in [1, 2]:
    #  dg.create_table(1000 * 1000 * 1000, 250 * 1000 * 1000, dist, i, True)
#
#  for i in [1, 2]:
#  dg.create_table(1000 * 1000 * 1000, 100 * 1000 * 1000, dist, i, True)
