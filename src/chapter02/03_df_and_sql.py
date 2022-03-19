from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('SparkApp').getOrCreate()

flightData2015 = spark \
	.read \
	.option('inferSchema', 'true') \
	.option('header', 'true') \
	.csv('../../data/flight-data/csv/2015-summary.csv')

# createOrReplaceTempView 를 사용하면 모든 DataFrame을 테이블이나 뷰로 만들 수 있다.
flightData2015.createOrReplaceTempView('flight_data_2015')

# SQL 방식
sqlWay = spark.sql('''
SELECT DEST_COUNTRY_NAME, count(1)
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
''')

# DataFrame 방식
dataFrameWay = flightData2015 \
	.groupby('DEST_COUNTRY_NAME') \
	.count()

# 두 방식은 동일한 실행 계획을 가진다
sqlWay.explain()
dataFrameWay.explain()

# 두 방식은 동일한 결과를 반환한다.
# -> [Row(max(count)=370002)]
sqlWay = spark.sql('SELECT max(count) FROM flight_data_2015').take(1)
dataFrameWay = flightData2015.select(max('count')).take(1)

###########
# 상위 5개의 도착 국가를 찾아내는 살짝 복잡한 쿼리
# 다중 트랜스포메이션이 발생한다
###########
# SQL 방식
maxSql = spark.sql('''
SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
ORDER BY sum(count) DESC 
LIMIT 5
''')
maxSql.show()

# DataFrame 방식
maxDataFrame = flightData2015 \
	.groupby('DEST_COUNTRY_NAME') \
	.sum('count') \
	.withColumnRenamed('sum(count)', 'destination_total') \
	.sort(desc('destination_total')) \
	.limit(5)
maxDataFrame.show()

# 실행 계획 분석해보기
maxDataFrame.explain()