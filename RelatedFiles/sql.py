from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, to_date
from pyspark.sql.types import StructType, StructField, IntegerType


spark = SparkSession.builder.appName('emr-spark-assignment').getOrCreate()

df1 = spark.read.csv('s3://emr-spark-assignment/voice_sample.csv', header=True, inferSchema=True)
df2 = spark.read.csv('s3://emr-spark-assignment/cell_centers.csv', header=True, inferSchema=True)

df1.createOrReplaceTempView("view_of_voice_sample")
df2.createOrReplaceTempView("view_of_cell_centers")

df1 = spark.sql('SELECT *, TO_DATE(CALL_TIME, "yyyyMMddHHmmss") AS CALL_DATE FROM view_of_voice_sample')

df1.createOrReplaceTempView("view_of_voice_sample")

df3 = spark.sql(
    '''
    SELECT CALLER_ID
    FROM (
        SELECT CALLER_ID, COUNT(DISTINCT CALL_DATE) AS COUNT_OF_DISTINCT_CALL_DATE
        FROM (
            SELECT CALLER_ID, CALL_DATE
            FROM view_of_voice_sample
            WHERE LOCATION_ID IN (SELECT LOCATION_ID FROM view_of_cell_centers WHERE PROVINCE_NAME = 'Western')
        )
        GROUP BY CALLER_ID
    )
    WHERE COUNT_OF_DISTINCT_CALL_DATE = (SELECT COUNT(DISTINCT CALL_DATE) FROM view_of_voice_sample)
    '''
)

df3.write.mode('overwrite').csv('s3://emr-spark-assignment/sql_output_csv/', header=True)

schema = StructType([StructField('COUNT', IntegerType())])
count_df = spark.createDataFrame([(df3.count(),)], schema)
count_df.write.mode('overwrite').csv('s3://emr-spark-assignment/sql_output_count/', header=True)

spark.stop()
