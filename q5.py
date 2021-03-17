
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *




spark = SparkSession.builder.master("local[1]") \
							.appName("Q5_a") \
							.getOrCreate()
spark.sparkContext.setLogLevel("WARN") # to turn off annoying INFO log printed out on terminal

schema = StructType([\
    StructField("userid", IntegerType(), True),\
    StructField("age", IntegerType(), True),\
    StructField("gender", StringType(), True),\
	StructField("occupation", StringType(), True),\
	StructField("zipcode", IntegerType(), True)])
df_uuser = spark.read.format("csv").option("delimiter", "|").option("header","false").schema(schema).load("u.user")
df_uuser.createOrReplaceTempView('user')
df2= spark.sql('SELECT DISTINCT (occupation), COUNT(occupation) AS count from user GROUP BY occupation ORDER BY count DESC')
df2.show()
print (df2.collect())
