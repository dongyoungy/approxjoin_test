import timeit
setup = "import data_gen"
s = "data_gen.create_table_data(1000*1000*1000, 100*1000*1000, 'uniform', 1)"
print(timeit.timeit(stmt=s, setup=setup, number=1))
