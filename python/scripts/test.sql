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
select query, left_dist as T1, right_dist as T2, p, q, variance(estimate) as variance, avg(abs((actual - estimate) / actual)) as avg_per_error, stddev(estimate) / avg(actual), count(*)
from results
group by query, left_dist, right_dist, p, q
order by query, left_dist, right_dist, p;

-- metric 2: relative error (avg. percent error)
select query, left_dist as T1, right_dist as T2, p, q, variance(estimate) as variance, avg(abs((actual - estimate) / actual)) as avg_per_error, stddev(estimate) / avg(actual), count(*)
from results
group by query, left_dist, right_dist, p, q
order by query, p, left_dist, right_dist;

select query, left_dist as T1, right_dist as T2, p, q, variance(estimate) as variance, avg(abs((actual - estimate) / actual)) as avg_per_error, stddev(estimate) / avg(actual)
from results
where left_dist like 'powerlaw4%' or right_dist like 'powerlaw4%'
group by query, left_dist, right_dist, p, q
order by query, left_dist, right_dist, p;

select query, left_dist as T1, right_dist as T2, p, q, variance(estimate) as variance, avg(abs((actual - estimate) / actual)) as avg_per_error, stddev(estimate) / avg(actual)
from results
where left_dist like 'powerlaw5%' or right_dist like 'powerlaw5%'
group by query, left_dist, right_dist, p, q
order by query, left_dist, right_dist, p;

select query, left_dist as T1, right_dist as T2, p, q, variance(estimate) as variance, avg(abs((actual - estimate) / actual)) as avg_per_error, stddev(estimate) / avg(actual), count(*)
from results
where left_dist like 'powerlaw3%' or right_dist like 'powerlaw3%'
group by query, left_dist, right_dist, p, q
order by query, left_dist, right_dist, p;

-- raw
select *
from results
order by query, left_dist, right_dist, idx;

select * from results order by query, left_dist, right_dist, idx;

SELECT COUNT(*) FROM synthetic_100m.uniform3_1 t1 JOIN synthetic_100m.powerlaw3_2 t2 ON t1.col1 = t2.col1