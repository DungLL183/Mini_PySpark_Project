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
q2a = df.filter(df.y=='yes').count()/df.count()*100
print('Marketing success rate')
print(q2a)
q2b = df.filter(df.y=='no').count()/df.count()*100
print('Marketing failure rate')
print(q2b)
q3 = spark.sql("select max(age), min(age), avg(age) from df").show()
q4 = spark.sql("select avg(balance), percentile_approx(balance,0.5) as median_balance from df").show()
q5 = spark.sql("select age, count(*) as number from df where y='yes' group by age order by number desc").show()
q6 = spark.sql("select marital, count(*) as number from df where y='yes' group by marital order by number desc").show()
q7 = spark.sql("select age, marital, count(*) as number from df where y='yes' group by age, marital order by number desc").show()
