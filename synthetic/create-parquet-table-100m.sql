CREATE SCHEMA IF NOT EXISTS synthetic_100m;

 CREATE TABLE synthetic_100m.t_1_uniform1_1 STORED AS parquet AS SELECT * FROM synthetic_100m_text.t_1_uniform1_1;
 CREATE TABLE synthetic_100m.t_1_uniform1_2 STORED AS parquet AS SELECT * FROM synthetic_100m_text.t_1_uniform1_2;
 CREATE TABLE synthetic_100m.t_2_uniform2_1 STORED AS parquet AS SELECT * FROM synthetic_100m_text.t_2_uniform2_1;
 CREATE TABLE synthetic_100m.t_2_uniform2_2 STORED AS parquet AS SELECT * FROM synthetic_100m_text.t_2_uniform2_2;
 CREATE TABLE synthetic_100m.t_3_normal1_1 STORED AS parquet AS SELECT * FROM synthetic_100m_text.t_3_normal1_1;
 CREATE TABLE synthetic_100m.t_3_normal1_2 STORED AS parquet AS SELECT * FROM synthetic_100m_text.t_3_normal1_2;
 CREATE TABLE synthetic_100m.t_4_normal2_1 STORED AS parquet AS SELECT * FROM synthetic_100m_text.t_4_normal2_1;
 CREATE TABLE synthetic_100m.t_4_normal2_2 STORED AS parquet AS SELECT * FROM synthetic_100m_text.t_4_normal2_2;
 CREATE TABLE synthetic_100m.t_5_powerlaw1_1 STORED AS parquet AS SELECT * FROM synthetic_100m_text.t_5_powerlaw1_1;
 CREATE TABLE synthetic_100m.t_5_powerlaw1_2 STORED AS parquet AS SELECT * FROM synthetic_100m_text.t_5_powerlaw1_2;
 CREATE TABLE synthetic_100m.t_6_powerlaw2_1 STORED AS parquet AS SELECT * FROM synthetic_100m_text.t_6_powerlaw2_1;
 CREATE TABLE synthetic_100m.t_6_powerlaw2_2 STORED AS parquet AS SELECT * FROM synthetic_100m_text.t_6_powerlaw2_2;

COMPUTE STATS synthetic_100m.t_1_uniform1_1;
COMPUTE STATS synthetic_100m.t_1_uniform1_2;
COMPUTE STATS synthetic_100m.t_2_uniform2_1;
COMPUTE STATS synthetic_100m.t_2_uniform2_2;
COMPUTE STATS synthetic_100m.t_3_normal1_1;
COMPUTE STATS synthetic_100m.t_3_normal1_2;
COMPUTE STATS synthetic_100m.t_4_normal2_1;
COMPUTE STATS synthetic_100m.t_4_normal2_1;
COMPUTE STATS synthetic_100m.t_5_powerlaw1_1;
COMPUTE STATS synthetic_100m.t_5_powerlaw1_2;
COMPUTE STATS synthetic_100m.t_6_powerlaw2_1;
COMPUTE STATS synthetic_100m.t_6_powerlaw2_2;

-- CREATE TABLE synthetic_100m.uniform3_1 STORED AS parquet AS SELECT * FROM synthetic_100m_text.uniform3_1;
-- CREATE TABLE synthetic_100m.uniform3_2 STORED AS parquet AS SELECT * FROM synthetic_100m_text.uniform3_2;
-- CREATE TABLE synthetic_100m.normal3_1 STORED AS parquet AS SELECT * FROM synthetic_100m_text.normal3_1;
-- CREATE TABLE synthetic_100m.normal3_2 STORED AS parquet AS SELECT * FROM synthetic_100m_text.normal3_2;
-- CREATE TABLE synthetic_100m.powerlaw3_1 STORED AS parquet AS SELECT * FROM synthetic_100m_text.powerlaw3_1;
-- CREATE TABLE synthetic_100m.powerlaw3_2 STORED AS parquet AS SELECT * FROM synthetic_100m_text.powerlaw3_2;
-- CREATE TABLE synthetic_100m.powerlaw4_1 STORED AS parquet AS SELECT * FROM synthetic_100m_text.powerlaw4_1;
-- CREATE TABLE synthetic_100m.powerlaw4_2 STORED AS parquet AS SELECT * FROM synthetic_100m_text.powerlaw4_2;
-- COMPUTE STATS synthetic_100m.uniform3_1;
-- COMPUTE STATS synthetic_100m.uniform3_2;
-- COMPUTE STATS synthetic_100m.normal3_1;
-- COMPUTE STATS synthetic_100m.normal3_2;
-- COMPUTE STATS synthetic_100m.powerlaw3_1;
-- COMPUTE STATS synthetic_100m.powerlaw3_2;
-- COMPUTE STATS synthetic_100m.powerlaw4_1;
-- COMPUTE STATS synthetic_100m.powerlaw4_2;

--  CREATE TABLE synthetic_100m.uniform1_1 STORED AS parquet AS SELECT * FROM synthetic_100m_text.uniform1_1;
--  CREATE TABLE synthetic_100m.uniform1_2 STORED AS parquet AS SELECT * FROM synthetic_100m_text.uniform1_2;
--  CREATE TABLE synthetic_100m.uniform2_1 STORED AS parquet AS SELECT * FROM synthetic_100m_text.uniform2_1;
--  CREATE TABLE synthetic_100m.uniform2_2 STORED AS parquet AS SELECT * FROM synthetic_100m_text.uniform2_2;
--  CREATE TABLE synthetic_100m.normal1_1 STORED AS parquet AS SELECT * FROM synthetic_100m_text.normal1_1;
--  CREATE TABLE synthetic_100m.normal1_2 STORED AS parquet AS SELECT * FROM synthetic_100m_text.normal1_2;
--  CREATE TABLE synthetic_100m.normal2_1 STORED AS parquet AS SELECT * FROM synthetic_100m_text.normal2_1;
--  CREATE TABLE synthetic_100m.normal2_2 STORED AS parquet AS SELECT * FROM synthetic_100m_text.normal2_2;
--  CREATE TABLE synthetic_100m.powerlaw1_1 STORED AS parquet AS SELECT * FROM synthetic_100m_text.powerlaw1_1;
--  CREATE TABLE synthetic_100m.powerlaw1_2 STORED AS parquet AS SELECT * FROM synthetic_100m_text.powerlaw1_2;
--  CREATE TABLE synthetic_100m.powerlaw2_1 STORED AS parquet AS SELECT * FROM synthetic_100m_text.powerlaw2_1;
--  CREATE TABLE synthetic_100m.powerlaw2_2 STORED AS parquet AS SELECT * FROM synthetic_100m_text.powerlaw2_2;


-- COMPUTE STATS synthetic_100m.uniform1_1;
-- COMPUTE STATS synthetic_100m.uniform1_2;
-- COMPUTE STATS synthetic_100m.uniform2_1;
-- COMPUTE STATS synthetic_100m.uniform2_2;
-- COMPUTE STATS synthetic_100m.normal1_1;
-- COMPUTE STATS synthetic_100m.normal1_2;
-- COMPUTE STATS synthetic_100m.normal2_1;
-- COMPUTE STATS synthetic_100m.normal2_1;
-- COMPUTE STATS synthetic_100m.powerlaw1_1;
-- COMPUTE STATS synthetic_100m.powerlaw1_2;
-- COMPUTE STATS synthetic_100m.powerlaw2_1;
-- COMPUTE STATS synthetic_100m.powerlaw2_2;

