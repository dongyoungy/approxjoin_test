CREATE DATABASE IF NOT EXISTS synthetic_small_text;

CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_small_text.uniform_1 (
  col1  INT,
  col2  DOUBLE,
  col3  INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/tmp/synthetic_small/uniform_1';
--  --
CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_small_text.uniform_2 (
  col1  INT,
  col2  DOUBLE,
  col3  INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/tmp/synthetic_small/uniform_2';

--  CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_small_text.normal_1 (
  --  col1  INT,
  --  col2  INT,
  --  col3  INT
--  )
--  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
--  LOCATION '/tmp/synthetic_small/normal_1';
--
--  CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_small_text.normal_2 (
  --  col1  INT,
  --  col2  INT,
  --  col3  INT
--  )
--  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
--  LOCATION '/tmp/synthetic_small/normal_2';
--
--  CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_small_text.powerlaw_1 (
  --  col1  INT,
  --  col2  INT,
  --  col3  INT
--  )
--  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
--  LOCATION '/tmp/synthetic_small/powerlaw_1';
--
--  CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_small_text.powerlaw_2 (
  --  col1  INT,
  --  col2  INT,
  --  col3  INT
--  )
--  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
--  LOCATION '/tmp/synthetic_small/powerlaw_2';
--
--  CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_small_text.uniform_uniform_1 (
  --  col1  INT,
  --  col2  INT,
  --  col3  INT
--  )
--  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
--  LOCATION '/tmp/synthetic_small/uniform_uniform_1';
--
--  CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_small_text.uniform_normal_1 (
  --  col1  INT,
  --  col2  INT,
  --  col3  INT
--  )
--  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
--  LOCATION '/tmp/synthetic_small/uniform_normal_1';
--
--  CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_small_text.uniform_powerlaw_1 (
  --  col1  INT,
  --  col2  INT,
  --  col3  INT
--  )
--  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
--  LOCATION '/tmp/synthetic_small/uniform_powerlaw_1';

--  CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_small_text.normal_powerlaw_1 (
  --  col1  INT,
  --  col2  INT,
  --  col3  INT
--  )
--  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
--  LOCATION '/tmp/synthetic_small/normal_powerlaw_1';

--
--  CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_small_text.uniform_max_var_2 (
  --  col1  INT,
  --  col2  INT,
  --  col3  INT
--  )
--  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
--  LOCATION '/tmp/synthetic_small/uniform_max_var_2';
--
--  CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_small_text.normal_max_var_2 (
  --  col1  INT,
  --  col2  INT,
  --  col3  INT
--  )
--  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
--  LOCATION '/tmp/synthetic_small/normal_max_var_2';
--
--  CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_small_text.powerlaw_max_var_2 (
  --  col1  INT,
  --  col2  INT,
  --  col3  INT
--  )
--  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
--  LOCATION '/tmp/synthetic_small/powerlaw_max_var_2';
