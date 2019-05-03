from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *


if __name__ == "__main__":
    sc = SparkContext(appName="CSV2Parquet")
    sqlContext = SQLContext(sc)

    schema = StructType([
            StructField("r_app_bundle", StringType(), True),
            StructField("r_app_name", StringType(), True),
            StructField("r_device_geo_city", StringType(), True),
            StructField("r_device_geo_region", StringType(), True),
            StructField("r_device_geo_zip", StringType(), True),
            StructField("r_device_geo_country", StringType(), True),
            StructField("cost", StringType(), True),
            StructField("rpm", StringType(), True),
            StructField("request_type", StringType(), True),
            StructField("r_device_osv", StringType(), True),
            StructField("r_device_connectiontype", StringType(), True),
            StructField("r_device_carrier", StringType(), True),
            StructField("h", StringType(), True),
            StructField("r_source_pchain", StringType(), True),
            StructField("r_bidswitchreadext_ssp", StringType(), True)])

    rdd = sc.textFile("main.csv").map(lambda line: line.split(","))
    df = sqlContext.createDataFrame(rdd, schema)
    df.write.parquet('./parquet')
