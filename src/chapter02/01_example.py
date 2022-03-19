from pyspark.sql import *

spark = SparkSession.builder.appName('Spark').getOrCreate()

myRange = spark.range(1000).toDF("number")
divisBy2 = myRange.where("number % 2 = 0")
divisBy2.show()
