CREATE DATABASE IF NOT EXISTS synthetic_text;

CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_text.uniform_1 (
  col1  INT,
  col2  INT,
  col3  INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/tmp/synthetic/uniform_1';

CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_text.uniform_2 (
  col1  INT,
  col2  INT,
  col3  INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/tmp/synthetic/uniform_2';

CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_text.normal_1 (
  col1  INT,
  col2  INT,
  col3  INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/tmp/synthetic/normal_1';

CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_text.normal_2 (
  col1  INT,
  col2  INT,
  col3  INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/tmp/synthetic/normal_2';

CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_text.powerlaw_1 (
  col1  INT,
  col2  INT,
  col3  INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/tmp/synthetic/powerlaw_1';

CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_text.powerlaw_2 (
  col1  INT,
  col2  INT,
  col3  INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/tmp/synthetic/powerlaw_2';

CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_text.uniform_uniform_1 (
  col1  INT,
  col2  INT,
  col3  INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/tmp/synthetic/uniform_uniform_1';

CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_text.uniform_normal_1 (
  col1  INT,
  col2  INT,
  col3  INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/tmp/synthetic/uniform_normal_1';

CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_text.uniform_powerlaw_1 (
  col1  INT,
  col2  INT,
  col3  INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/tmp/synthetic/uniform_powerlaw_1';

CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_text.uniform_max_var_2 (
  col1  INT,
  col2  INT,
  col3  INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/tmp/synthetic/uniform_max_var_2';

CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_text.normal_max_var_2 (
  col1  INT,
  col2  INT,
  col3  INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/tmp/synthetic/normal_max_var_2';

CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_text.powerlaw_max_var_2 (
  col1  INT,
  col2  INT,
  col3  INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/tmp/synthetic/powerlaw_max_var_2';
