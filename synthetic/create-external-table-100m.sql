CREATE DATABASE IF NOT EXISTS synthetic_100m_text;

 CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_100m_text.t_1_uniform1_1 (
   col1  INT,
   col2  INT,
   col3  INT
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
 LOCATION '/tmp/synthetic_100m/1_uniform1_1';

 CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_100m_text.t_1_uniform1_2 (
   col1  INT,
   col2  INT,
   col3  INT
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
 LOCATION '/tmp/synthetic_100m/1_uniform1_2';

 CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_100m_text.t_2_uniform2_1 (
   col1  INT,
   col2  INT,
   col3  INT
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
 LOCATION '/tmp/synthetic_100m/2_uniform2_1';

 CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_100m_text.t_2_uniform2_2 (
   col1  INT,
   col2  INT,
   col3  INT
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
 LOCATION '/tmp/synthetic_100m/2_uniform2_2';

 CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_100m_text.t_3_normal1_1 (
   col1  INT,
   col2  INT,
   col3  INT
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
 LOCATION '/tmp/synthetic_100m/3_normal1_1';

 CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_100m_text.t_3_normal1_2 (
   col1  INT,
   col2  INT,
   col3  INT
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
 LOCATION '/tmp/synthetic_100m/3_normal1_2';

 CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_100m_text.t_4_normal2_1 (
   col1  INT,
   col2  INT,
   col3  INT
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
 LOCATION '/tmp/synthetic_100m/4_normal2_1';

 CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_100m_text.t_4_normal2_2 (
   col1  INT,
   col2  INT,
   col3  INT
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
 LOCATION '/tmp/synthetic_100m/4_normal2_2';


CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_100m_text.t_5_powerlaw1_1 (
   col1  INT,
   col2  INT,
   col3  INT
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
 LOCATION '/tmp/synthetic_100m/5_powerlaw1_1';

 CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_100m_text.t_5_powerlaw1_2 (
   col1  INT,
   col2  INT,
   col3  INT
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
 LOCATION '/tmp/synthetic_100m/5_powerlaw1_2';

 CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_100m_text.t_6_powerlaw2_1 (
   col1  INT,
   col2  INT,
   col3  INT
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
 LOCATION '/tmp/synthetic_100m/6_powerlaw2_1';

 CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_100m_text.t_6_powerlaw2_2 (
   col1  INT,
   col2  INT,
   col3  INT
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
 LOCATION '/tmp/synthetic_100m/6_powerlaw2_2';


--  CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_100m_text.uniform5_1 (
--    col1  INT,
--    col2  INT,
--    col3  INT
--  )
--  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
--  LOCATION '/tmp/synthetic_100m/uniform5_1';

--  CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_100m_text.uniform5_2 (
--    col1  INT,
--    col2  INT,
--    col3  INT
--  )
--  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
--  LOCATION '/tmp/synthetic_100m/uniform5_2';

--  CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_100m_text.normal5_1 (
--    col1  INT,
--    col2  INT,
--    col3  INT
--  )
--  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
--  LOCATION '/tmp/synthetic_100m/normal5_1';

--  CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_100m_text.normal5_2 (
--    col1  INT,
--    col2  INT,
--    col3  INT
--  )
--  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
--  LOCATION '/tmp/synthetic_100m/normal5_2';

--  CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_100m_text.powerlaw5_1 (
--    col1  INT,
--    col2  INT,
--    col3  INT
--  )
--  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
--  LOCATION '/tmp/synthetic_100m/powerlaw5_1';

--  CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_100m_text.powerlaw5_2 (
--    col1  INT,
--    col2  INT,
--    col3  INT
--  )
--  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
--  LOCATION '/tmp/synthetic_100m/powerlaw5_2';