CREATE DATABASE IF NOT EXISTS synthetic_10m_text;

--  CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_10m_text.uniform_1 (
  --  col1  INT,
  --  col2  INT,
  --  col3  INT
--  )
--  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
--  LOCATION '/tmp/synthetic_10m/uniform_1';
--
--  CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_10m_text.uniform_2 (
  --  col1  INT,
  --  col2  INT,
  --  col3  INT
--  )
--  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
--  LOCATION '/tmp/synthetic_10m/uniform_2';
--
--  CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_10m_text.normal_1 (
  --  col1  INT,
  --  col2  INT,
  --  col3  INT
--  )
--  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
--  LOCATION '/tmp/synthetic_10m/normal_1';
--
--  CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_10m_text.normal_2 (
  --  col1  INT,
  --  col2  INT,
  --  col3  INT
--  )
--  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
--  LOCATION '/tmp/synthetic_10m/normal_2';
--
--  CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_10m_text.powerlaw2_1 (
--    col1  INT,
--    col2  INT,
--    col3  INT
--  )
--  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
--  LOCATION '/tmp/synthetic_10m/powerlaw2_1';

 CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_10m_text.powerlaw2_2 (
   col1  INT,
   col2  INT,
   col3  INT
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
 LOCATION '/tmp/synthetic_10m/powerlaw2_2';


 CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_10m_text.powerlaw3_1 (
   col1  INT,
   col2  INT,
   col3  INT
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
 LOCATION '/tmp/synthetic_10m/powerlaw3_1';

 CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_10m_text.powerlaw3_2 (
   col1  INT,
   col2  INT,
   col3  INT
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
 LOCATION '/tmp/synthetic_10m/powerlaw3_2';

--  CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_10m_text.uniform_uniform_1 (
  --  col1  INT,
  --  col2  INT,
  --  col3  INT
--  )
--  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
--  LOCATION '/tmp/synthetic_10m/uniform_uniform_1';
--
--  CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_10m_text.uniform_normal_1 (
  --  col1  INT,
  --  col2  INT,
  --  col3  INT
--  )
--  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
--  LOCATION '/tmp/synthetic_10m/uniform_normal_1';
--
--  CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_10m_text.uniform_powerlaw_1 (
  --  col1  INT,
  --  col2  INT,
  --  col3  INT
--  )
--  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
--  LOCATION '/tmp/synthetic_10m/uniform_powerlaw_1';

-- CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_10m_text.normal_powerlaw_1 (
--   col1  INT,
--   col2  INT,
--   col3  INT
-- )
-- ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
-- LOCATION '/tmp/synthetic_10m/normal_powerlaw_1';

--  CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_10m_text.uniform_max_var_2 (
  --  col1  INT,
  --  col2  INT,
  --  col3  INT
--  )
--  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
--  LOCATION '/tmp/synthetic_10m/uniform_max_var_2';
--
--  CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_10m_text.normal_max_var_2 (
  --  col1  INT,
  --  col2  INT,
  --  col3  INT
--  )
--  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
--  LOCATION '/tmp/synthetic_10m/normal_max_var_2';
--
--  CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_10m_text.powerlaw_max_var_2 (
  --  col1  INT,
  --  col2  INT,
  --  col3  INT
--  )
--  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
--  LOCATION '/tmp/synthetic_10m/powerlaw_max_var_2';
