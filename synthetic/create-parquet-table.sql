CREATE SCHEMA IF NOT EXISTS synthetic;
CREATE TABLE synthetic.uniform_1 STORED AS parquet AS SELECT * FROM synthetic_text.uniform_1;
CREATE TABLE synthetic.uniform_2 STORED AS parquet AS SELECT * FROM synthetic_text.uniform_2;
CREATE TABLE synthetic.normal_1 STORED AS parquet AS SELECT * FROM synthetic_text.normal_1;
CREATE TABLE synthetic.normal_2 STORED AS parquet AS SELECT * FROM synthetic_text.normal_2;
CREATE TABLE synthetic.powerlaw_1 STORED AS parquet AS SELECT * FROM synthetic_text.powerlaw_1;
CREATE TABLE synthetic.powerlaw_2 STORED AS parquet AS SELECT * FROM synthetic_text.powerlaw_2;
CREATE TABLE synthetic.uniform_uniform_1 STORED AS parquet AS SELECT * FROM synthetic_text.uniform_uniform_1;
CREATE TABLE synthetic.uniform_normal_1 STORED AS parquet AS SELECT * FROM synthetic_text.uniform_normal_1;
CREATE TABLE synthetic.uniform_powerlaw_1 STORED AS parquet AS SELECT * FROM synthetic_text.uniform_powerlaw_1;
CREATE TABLE synthetic.uniform_max_var_2 STORED AS parquet AS SELECT * FROM synthetic_text.uniform_max_var_2;
CREATE TABLE synthetic.normal_max_var_2 STORED AS parquet AS SELECT * FROM synthetic_text.normal_max_var_2;
CREATE TABLE synthetic.powerlaw_max_var_2 STORED AS parquet AS SELECT * FROM synthetic_text.powerlaw_max_var_2;


COMPUTE STATS synthetic.uniform_1;
COMPUTE STATS synthetic.uniform_2;
COMPUTE STATS synthetic.normal_1;
COMPUTE STATS synthetic.normal_2;
COMPUTE STATS synthetic.powerlaw_1;
COMPUTE STATS synthetic.powerlaw_2;
COMPUTE STATS synthetic.uniform_uniform_1;
COMPUTE STATS synthetic.uniform_normal_1;
COMPUTE STATS synthetic.uniform_powerlaw_1;
COMPUTE STATS synthetic.uniform_max_var_2;
COMPUTE STATS synthetic.normal_max_var_2;
COMPUTE STATS synthetic.powerlaw_max_var_2;

