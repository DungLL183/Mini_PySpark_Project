import findspark
findspark.init()
import pyspark
from pyspark.sql import *
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('project.com').getOrCreate()
sc = spark.sparkContext
df = spark.read.options(header='True', inferSchema='True', delimiter=';').csv("/home/Ubuntu/Bank.csv")
df.createOrReplaceTempView("df")
df.createGlobalTempView("df")
q8 = spark.sql("select distinct(job) as job_balance_greater_1000 from df where balance > 1000 ").show()
q9 = spark.sql("select education , count(*) as no_of_cust from df group by education").show()
q10 = spark.sql("select count(*) as rich_but_homeless from df where balance > 800 and housing = 'no' ").show()
q11 = spark.sql("select distinct(job), count(*) as no_of_job from df group by job order by 2 desc ").show()
q12 = spark.sql("select count(*) as subscribed_as_default from df where y='yes' and default = 'yes' ").show()
