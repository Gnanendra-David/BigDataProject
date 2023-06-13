#get a dataset from kaggle atleast 100mb 

#move this dataset from local system (computer) to cloudx local

scp /Users/david/Downloads/quotes.csv
bigdatacloudxlab27228@f.cloudxlab.com:/home/bigdatacloudxlab27228/quotes_landing

#move this from cloudx local to hdfs

hdfs dfs -mkdir quotes_landing 
hdfs dfs -copyFromLocal /home/bigdatacloudxlab39242/gnanendra_project/quotes.csv /user/bigdatacloudxlab27228/quotes_landing

#create external table on top of the hdfs file using hive
#in cloudx lab press 'hive'

Create database quotes
Use quotes;
CREATE EXTERNAL TABLE `quotes`(
  `quote` string COMMENT 'from deserializer', 
  `author` string COMMENT 'from deserializer', 
  `category` string COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'quoteChar'='\"', 
  'separatorChar'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://cxln1.c.thelab-240901.internal:8020/user/bigdatacloudxlab39242/gnan0315'
TBLPROPERTIES (
  'numFiles'='1', 
  'skip.header.line.count'='1', 
  'totalSize'='144428641', 
  'transient_lastDdlTime'='1686003181')

---------------
create database quotesdb
use quotesdb
CREATE EXTERNAL TABLE quotestb (
    quote string,
    author string,
    category string
    
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '\"'
)
LOCATION '/user/bigdatacloudxlab27228/quotes_landing'
TBLPROPERTIES ("skip.header.line.count"="1");

#reading the data into df using pyspark
#in cloudx click 'pyspark'
----
import pyspark
import pyspark.sql.functions
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('sparkdf').enableHiveSupport().getOrCreate()
----

from pyspark.sql.functions import col
spark.sql(“use quotesdb”)
df=spark.sql(“select * from quotestb limit 10”)

#replace null values with n/a
df=df.fillna('N/A')

#removing the header value
df = df.filter(col("quote") != "quote")

#converting into parquet
df.write.mode("overwrite").parquet("/user/bigdatacloudxlab27228/quotes_landing1")

#again converting it into csv to sqoop it
df.write.mode("overwrite").csv("/user/bigdatacloudxlab39242/quotes_landing1")

#before doing sqoop find out about port number etc 
#mysql
SHOW VARIABLES LIKE 'hostname';
SHOW VARIABLES LIKE 'port'; 

#in clouxlab

hdfs getconf -confKey fs.defaultFS type in linux terminal

#create a table in mysql
CREATE TABLE quotestb (
    quote VARCHAR(255),
    author VARCHAR(255),
    category VARCHAR(255)
)

#now sqoop it in mysql
sqoop export \
--connect jdbc:mysql://cxln2:3306/sqoopex \
--username sqoopuser \
--password NHkkP876rp \
--table quotestb \
--export-dir hdfs://cxln1.c.thelab-240901.internal:8020/user/bigdatacloudxlab39242/quotes_landing1 \
--input-fields-terminated-by ',' \
--lines-terminated-by '\n' \
--m 1

sqoop export \
--connect jdbc:mysql://cxln2:3306/sqoopex \
--username 'sqoopuser' \
--password 'NHkkP876rp' \
--table quotestb \
--export-dir hdfs://cxln1.c.thelab-240901.internal:8020/user/bigdatacloudxlab27228/quotes_landing_csv \
--input-fields-terminated-by ',' \
--input-lines-terminated-by '\n' \
-m1

######
#going through disk quota exceeded error##
hdfs dfs -du-h /user/xyz | grep 'M"

du -h --max-depth=1 | grep 'M"

#during email conversations#
##avoid a follow up questions##
#sometimes overcommunicate better#
#dont over explain#

#this is new