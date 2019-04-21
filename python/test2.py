import csv
import numpy as np

a = np.array([1, 2, 3])
b = np.array([4, 5, 6])
a.shape = (3, 1)
b.shape = (3, 1)

keys = np.array(np.random.randint(1, 10000, 3))
val1 = np.array(np.random.normal(100, 25, 3))
val2 = np.array(np.random.randint(1, 100, 3))

keys.shape = (3, 1)
val1.shape = (3, 1)
val2.shape = (3, 1)

c = np.hstack((keys, val1, val2))

with open('names.csv', 'w') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerows(c)
