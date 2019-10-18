CREATE SCHEMA IF NOT EXISTS synthetic_small;
CREATE TABLE synthetic_small.uniform_1 STORED AS parquet AS SELECT * FROM synthetic_small_text.uniform_1;
CREATE TABLE synthetic_small.uniform_2 STORED AS parquet AS SELECT * FROM synthetic_small_text.uniform_2;
--  CREATE TABLE synthetic_small.normal_1 STORED AS parquet AS SELECT * FROM synthetic_small_text.normal_1;
--  CREATE TABLE synthetic_small.normal_2 STORED AS parquet AS SELECT * FROM synthetic_small_text.normal_2;
--  CREATE TABLE synthetic_small.powerlaw_1 STORED AS parquet AS SELECT * FROM synthetic_small_text.powerlaw_1;
--  CREATE TABLE synthetic_small.powerlaw_2 STORED AS parquet AS SELECT * FROM synthetic_small_text.powerlaw_2;
--  CREATE TABLE synthetic_small.uniform_uniform_1 STORED AS parquet AS SELECT * FROM synthetic_small_text.uniform_uniform_1;
--  CREATE TABLE synthetic_small.uniform_normal_1 STORED AS parquet AS SELECT * FROM synthetic_small_text.uniform_normal_1;
--  CREATE TABLE synthetic_small.uniform_powerlaw_1 STORED AS parquet AS SELECT * FROM synthetic_small_text.uniform_powerlaw_1;
--  CREATE TABLE synthetic_small.normal_powerlaw_1 STORED AS parquet AS SELECT * FROM synthetic_small_text.normal_powerlaw_1;
--  CREATE TABLE synthetic_small.uniform_max_var_2 STORED AS parquet AS SELECT * FROM synthetic_small_text.uniform_max_var_2;
--  CREATE TABLE synthetic_small.normal_max_var_2 STORED AS parquet AS SELECT * FROM synthetic_small_text.normal_max_var_2;
--  CREATE TABLE synthetic_small.powerlaw_max_var_2 STORED AS parquet AS SELECT * FROM synthetic_small_text.powerlaw_max_var_2;


COMPUTE STATS synthetic_small.uniform_1;
COMPUTE STATS synthetic_small.uniform_2;
--  COMPUTE STATS synthetic_small.normal_1;
--  COMPUTE STATS synthetic_small.normal_2;
--  COMPUTE STATS synthetic_small.powerlaw_1;
--  COMPUTE STATS synthetic_small.powerlaw_2;
--  COMPUTE STATS synthetic_small.uniform_uniform_1;
--  COMPUTE STATS synthetic_small.uniform_normal_1;
--  COMPUTE STATS synthetic_small.uniform_powerlaw_1;
--  COMPUTE STATS synthetic_small.normal_powerlaw_1;
--  COMPUTE STATS synthetic_small.uniform_max_var_2;
--  COMPUTE STATS synthetic_small.normal_max_var_2;
--  COMPUTE STATS synthetic_small.powerlaw_max_var_2;

