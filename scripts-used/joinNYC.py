# Use the following command to run this code on spark cluster running over yarn
# spark-submit --master yarn --deploy-mode cluster --packages com.databricks:spark-csv_2.10:1.4.0 script.py \ 
# wasb://<containerName>@<storageAccount>.blob.core.windows.net/<path to the csv trip dataset> \ 
# wasb://<containerName>@<storageAccount>.blob.core.windows.net/<path to the csv fare dataset>  
from pyspark.sql.types import *;from pyspark.sql.functions import *
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import argparse


if __name__ == "__main__":

        parser = argparse.ArgumentParser()
        parser.add_argument('tripData', help='wasb Path of the trip csv file')
        parser.add_argument('tripFare', help='wasb Path of the fare csv file')
        parser.add_argument('outputCSVFile', help='CSV output file')
        args = parser.parse_args()
        sc = SparkContext()
        sqlContext = SQLContext(sc)
        sc._jsc.hadoopConfiguration().set('mapreduce.fileoutputcommitter.marksuccessfuljob','false')



        CustomSchemaTripData = StructType([StructField("medallion",StringType(),True),
            StructField("hack_license",StringType(),True),
            StructField("vendor_id",StringType(),True),
            StructField("rate_code",StringType(),True),
            StructField("store_and_fwd_flag",StringType(),True),
            StructField("pickup_datetime",StringType(),True),
            StructField("dropoff_datetime",StringType(),True),
            StructField("passenger_count",IntegerType(),True),
            StructField("trip_time_in_secs",DoubleType(),True),
            StructField("trip_distance",DoubleType(),True),
            StructField("pickup_longitude",DoubleType(),True),
            StructField("pickup_latitude",DoubleType(),True),
            StructField("dropoff_longitude",DoubleType(),True),
            StructField("dropoff_latitude",DoubleType(),True)])

        tripDf = sqlContext.read.format('com.databricks.spark.csv').options(header="true",
            delimiter=',',dateFormat="yyyy-MM-dd'T'HH:mm:ss.SSS").load(args.tripData, schema=CustomSchemaTripData)


        tripDf.registerTempTable('T')

        CustomSchemaFares = StructType([StructField("medallion",StringType(),True),
            StructField("hack_license",StringType(),True),
            StructField("vendor_id",StringType(),True),
            StructField("pickup_datetime",StringType(),True),
            StructField("payment_type",StringType(),True),
            StructField("fare_amount",DoubleType(),True),
            StructField("surcharge",DoubleType(),True),
            StructField("mta_tax",DoubleType(),True),
            StructField("tip_amount",DoubleType(),True),
            StructField("tolls_amount",DoubleType(),True),
            StructField("total_amount",DoubleType(),True)])


        faresDf = sqlContext.read.format('com.databricks.spark.csv').options(header="true",
            delimiter=',',dateFormat="yyyy-MM-dd'T'HH:mm:ss.SSS").load(args.tripFare, schema=CustomSchemaFares)

        faresDf.registerTempTable('F')

        final  = sqlContext.sql("select T.medallion, \
			T.hack_license,\
			T.vendor_id, \
			T.rate_code, \
			T.store_and_fwd_flag,\
			T.dropoff_datetime, \
			T.pickup_datetime, \
			hour(T.pickup_datetime) as pickup_hour,\
            weekofyear(T.pickup_datetime) as pickup_week,\
            from_unixtime(unix_timestamp(T.pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),'u') as pickup_weekday,\
			T.passenger_count, \
			T.trip_time_in_secs, \
			T.trip_distance, \
			T.pickup_longitude,\
            T.pickup_latitude,\
            T.dropoff_longitude,\
            T.dropoff_latitude,\
            F.payment_type,\
            F.fare_amount,\
            F.surcharge,\
            F.mta_tax,\
            F.tip_amount,\
            F.tolls_amount,\
            F.total_amount from T JOIN F on T.medallion = F.medallion and T.hack_license = F.hack_license and T.pickup_datetime = F.pickup_datetime")




        print final.count()
        final.write.format('com.databricks.spark.csv').mode('overwrite').options(header="true",
            delimiter=',',dateFormat="yyyy-MM-dd'T'HH:mm:ss.SSS").save(args.outputCSVFile)

