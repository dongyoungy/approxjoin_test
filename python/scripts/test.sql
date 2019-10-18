select query, p, q, variance(estimate)
from results
group by query, p, q
order by query, p;

-- metric 1: standard deviation / actual
select query, p, q, stddev(estimate) / avg(actual)
from results
group by query, p, q
order by query, p;

-- metric 2: relative error (avg. percent error)
select query, p, q, avg(abs((actual - estimate) / actual) * 100)
from results
group by query, p, q
order by query, p;

-- metric 3: 1.96 * sigma / sqrt(n) / bar(x)
select query, p, q, (1.96 * stddev(estimate) / sqrt(count(*))) / avg(estimate)
from results
group by query, p, q
order by query, p;

-- for synthetic
select query, left_dist, right_dist, p, q, variance(estimate)
from results
group by query, left_dist, right_dist, p, q
order by query, left_dist, right_dist, p;

-- metric 1: standard deviation / actual
select query, left_dist, right_dist, p, q, stddev(estimate) / avg(actual), avg(estimate)
from results
group by query, left_dist, right_dist, p, q
order by query, left_dist, right_dist, p;

-- metric 2: relative error (avg. percent error)
select query, left_dist as T1, right_dist as T2, p, q, avg(abs((actual - estimate) / actual) * 100) as avg_per_error
from results
group by query, left_dist, right_dist, p, q
order by query, left_dist, right_dist, p;

-- metric 3: 1.96 * sigma / sqrt(n) / bar(x)
select query, left_dist, right_dist, p, q, (1.96 * stddev(estimate) / sqrt(count(*))) / avg(estimate)
from results
group by query, left_dist, right_dist, p, q
order by query, left_dist, right_dist, p;

-- all in one
select query, left_dist, right_dist, p, q, stddev(estimate) / avg(actual), avg(abs((actual - estimate) / actual) * 100), (1.96 * stddev(estimate) / sqrt(count(*))) / avg(estimate)
from results
group by query, left_dist, right_dist, p, q
order by query, left_dist, right_dist, p;

-- metric 2: relative error (avg. percent error)
select query, left_dist as T1, right_dist as T2, p, q, variance(estimate) as variance, avg(abs((actual - estimate) / actual)) as avg_per_error, stddev(estimate) / avg(actual)
from results
group by query, left_dist, right_dist, p, q
order by query, left_dist, right_dist, p;

-- raw
select *
from results
order by query, left_dist, right_dist, idx;