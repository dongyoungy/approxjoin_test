CREATE SCHEMA IF NOT EXISTS synthetic_1m_2;
CREATE TABLE synthetic_1m_2.uniform_1 STORED AS parquet AS SELECT * FROM synthetic_1m_2_text.uniform_1;
CREATE TABLE synthetic_1m_2.uniform_2 STORED AS parquet AS SELECT * FROM synthetic_1m_2_text.uniform_2;
CREATE TABLE synthetic_1m_2.normal_1 STORED AS parquet AS SELECT * FROM synthetic_1m_2_text.normal_1;
CREATE TABLE synthetic_1m_2.normal_2 STORED AS parquet AS SELECT * FROM synthetic_1m_2_text.normal_2;
CREATE TABLE synthetic_1m_2.powerlaw_1 STORED AS parquet AS SELECT * FROM synthetic_1m_2_text.powerlaw_1;
CREATE TABLE synthetic_1m_2.powerlaw_2 STORED AS parquet AS SELECT * FROM synthetic_1m_2_text.powerlaw_2;

COMPUTE STATS synthetic_1m_2.uniform_1;
COMPUTE STATS synthetic_1m_2.uniform_2;
COMPUTE STATS synthetic_1m_2.normal_1;
COMPUTE STATS synthetic_1m_2.normal_2;
COMPUTE STATS synthetic_1m_2.powerlaw_1;
COMPUTE STATS synthetic_1m_2.powerlaw_2;

