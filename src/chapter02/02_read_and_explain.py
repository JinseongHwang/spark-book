from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkApp').getOrCreate()

# read : 좁은 트랜스포메이션
flightData2015 = spark \
	.read \
	.option('inferSchema', 'true') \
	.option('header', 'true') \
	.csv('../../data/flight-data/csv/2015-summary.csv')

# sort : 넓은 트랜스포메이션
flightData2015.sort("count").explain()  # 쿼리 실행 계획 분석

# 셔플 출력 파티션의 갯수를 200개(default)에서 5개로 변경
spark.conf.set('spark.sql.shuffle.partitions', '5')
res = flightData2015.sort('count').take(2)