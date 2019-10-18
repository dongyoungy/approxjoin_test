CREATE SCHEMA IF NOT EXISTS synthetic_10m;
--  CREATE TABLE synthetic_10m.uniform_1 STORED AS parquet AS SELECT * FROM synthetic_10m_text.uniform_1;
--  CREATE TABLE synthetic_10m.uniform_2 STORED AS parquet AS SELECT * FROM synthetic_10m_text.uniform_2;
--  CREATE TABLE synthetic_10m.normal_1 STORED AS parquet AS SELECT * FROM synthetic_10m_text.normal_1;
--  CREATE TABLE synthetic_10m.normal_2 STORED AS parquet AS SELECT * FROM synthetic_10m_text.normal_2;
--  CREATE TABLE synthetic_10m.powerlaw_1 STORED AS parquet AS SELECT * FROM synthetic_10m_text.powerlaw_1;
--  CREATE TABLE synthetic_10m.powerlaw_2 STORED AS parquet AS SELECT * FROM synthetic_10m_text.powerlaw_2;
--  CREATE TABLE synthetic_10m.powerlaw2_1 STORED AS parquet AS SELECT * FROM synthetic_10m_text.powerlaw2_1;
 CREATE TABLE synthetic_10m.powerlaw2_2 STORED AS parquet AS SELECT * FROM synthetic_10m_text.powerlaw2_2;
 CREATE TABLE synthetic_10m.powerlaw3_1 STORED AS parquet AS SELECT * FROM synthetic_10m_text.powerlaw3_1;
 CREATE TABLE synthetic_10m.powerlaw3_2 STORED AS parquet AS SELECT * FROM synthetic_10m_text.powerlaw3_2;
--  CREATE TABLE synthetic_10m.uniform_uniform_1 STORED AS parquet AS SELECT * FROM synthetic_10m_text.uniform_uniform_1;
--  CREATE TABLE synthetic_10m.uniform_normal_1 STORED AS parquet AS SELECT * FROM synthetic_10m_text.uniform_normal_1;
--  CREATE TABLE synthetic_10m.uniform_powerlaw_1 STORED AS parquet AS SELECT * FROM synthetic_10m_text.uniform_powerlaw_1;
-- CREATE TABLE synthetic_10m.normal_powerlaw_1 STORED AS parquet AS SELECT * FROM synthetic_10m_text.normal_powerlaw_1;
--  CREATE TABLE synthetic_10m.uniform_max_var_2 STORED AS parquet AS SELECT * FROM synthetic_10m_text.uniform_max_var_2;
--  CREATE TABLE synthetic_10m.normal_max_var_2 STORED AS parquet AS SELECT * FROM synthetic_10m_text.normal_max_var_2;
--  CREATE TABLE synthetic_10m.powerlaw_max_var_2 STORED AS parquet AS SELECT * FROM synthetic_10m_text.powerlaw_max_var_2;


--  COMPUTE STATS synthetic_10m.uniform_1;
--  COMPUTE STATS synthetic_10m.uniform_2;
--  COMPUTE STATS synthetic_10m.normal_1;
--  COMPUTE STATS synthetic_10m.normal_2;
--  COMPUTE STATS synthetic_10m.powerlaw_1;
--  COMPUTE STATS synthetic_10m.powerlaw_2;
--  COMPUTE STATS synthetic_10m.powerlaw2_1;
 COMPUTE STATS synthetic_10m.powerlaw2_2;
 COMPUTE STATS synthetic_10m.powerlaw3_1;
 COMPUTE STATS synthetic_10m.powerlaw3_2;
--  COMPUTE STATS synthetic_10m.uniform_uniform_1;
--  COMPUTE STATS synthetic_10m.uniform_normal_1;
--  COMPUTE STATS synthetic_10m.uniform_powerlaw_1;
-- COMPUTE STATS synthetic_10m.normal_powerlaw_1;
--  COMPUTE STATS synthetic_10m.uniform_max_var_2;
--  COMPUTE STATS synthetic_10m.normal_max_var_2;
--  COMPUTE STATS synthetic_10m.powerlaw_max_var_2;

