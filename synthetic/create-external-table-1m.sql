CREATE DATABASE IF NOT EXISTS synthetic_1m_2_text;

 CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_1m_2_text.uniform_1 (
   col1  INT,
   col2  INT,
   col3  INT
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
 LOCATION '/tmp/synthetic_1m_2/uniform_1';

 CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_1m_2_text.uniform_2 (
   col1  INT,
   col2  INT,
   col3  INT
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
 LOCATION '/tmp/synthetic_1m_2/uniform_2';

 CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_1m_2_text.normal_1 (
   col1  INT,
   col2  INT,
   col3  INT
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
 LOCATION '/tmp/synthetic_1m_2/normal_1';

 CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_1m_2_text.normal_2 (
   col1  INT,
   col2  INT,
   col3  INT
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
 LOCATION '/tmp/synthetic_1m_2/normal_2';

 CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_1m_2_text.powerlaw_1 (
   col1  INT,
   col2  INT,
   col3  INT
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
 LOCATION '/tmp/synthetic_1m_2/powerlaw_1';

 CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_1m_2_text.powerlaw_2 (
   col1  INT,
   col2  INT,
   col3  INT
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
 LOCATION '/tmp/synthetic_1m_2/powerlaw_2';
