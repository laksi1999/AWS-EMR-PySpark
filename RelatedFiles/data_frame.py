from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, to_date
from pyspark.sql.types import StructType, StructField, IntegerType


spark = SparkSession.builder.appName('emr-spark-assignment').getOrCreate()

df1 = spark.read.csv('s3://emr-spark-assignment/voice_sample.csv', header=True, inferSchema=True)
df2 = spark.read.csv('s3://emr-spark-assignment/cell_centers.csv', header=True, inferSchema=True)

df1 = df1.withColumn('CALL_DATE', to_date('CALL_TIME', 'yyyyMMddHHmmss'))

df3 = (
    df1
    .join(df2, ['LOCATION_ID'])
    .filter(df2['PROVINCE_NAME'] == 'Western')
    .groupBy('CALLER_ID').agg(countDistinct('CALL_DATE').alias('COUNT_OF_DISTINCT_CALL_DATE'))
    .filter(col('COUNT_OF_DISTINCT_CALL_DATE') == df1.select('CALL_DATE').distinct().count()).select('CALLER_ID')
)

df3.write.mode('overwrite').csv('s3://emr-spark-assignment/data_frame_output_csv/', header=True)

schema = StructType([StructField('COUNT', IntegerType())])
count_df = spark.createDataFrame([(df3.count(),)], schema)
count_df.write.mode('overwrite').csv('s3://emr-spark-assignment/data_frame_output_count/', header=True)

spark.stop()
