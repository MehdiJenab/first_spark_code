
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *




spark = SparkSession.builder.master("local[1]") \
							.appName("Q5_a") \
							.getOrCreate()
spark.sparkContext.setLogLevel("WARN") # to turn off annoying INFO log printed out on terminal

schema_user = StructType([\
    StructField("userid", IntegerType(), True),\
    StructField("age", IntegerType(), True),\
    StructField("gender", StringType(), True),\
	StructField("occupation", StringType(), True),\
	StructField("zipcode", IntegerType(), True)])
df_uuser = spark.read.format("csv").option("delimiter", "|").option("header","false").schema(schema_user).load("u.user")
df_uuser.createOrReplaceTempView('user')

schema_data = StructType([\
    StructField("userid", IntegerType(), True),\
    StructField("itemid", IntegerType(), True),\
    StructField("rating", IntegerType(), True),\
	StructField("time", IntegerType(), True)])
df_uuser = spark.read.format("csv").option("delimiter", "\t").option("header","false").schema(schema_data).load("u.data")
df_uuser.createOrReplaceTempView('data')


df2= spark.sql("""SELECT DISTINCT (u.occupation), COUNT(d.userid) 
	AS count from user AS u, data as d
	WHERE u.userid = d.userid AND u.occupation <> "other" AND u.occupation <> "none"
	GROUP BY occupation 
	ORDER BY count DESC""")

df2.show()
print (df2.collect())
