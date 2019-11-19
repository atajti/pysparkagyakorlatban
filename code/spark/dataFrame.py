#--------------------------------------------------------------------#
#                                                                    #
#                          PySpark dataFrames                        #
#                                                                    #
#--------------------------------------------------------------------#
#
# Taxi ride data can be downloaded from https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
# Data sits on HDFS's /tmp/taxi_rides folder.



taxi_data=spark.read.csv("/tmp/taxi_rides/", header = True)
taxi_data.count()
taxi_data.printSchema()

from pyspark.sql.types import DoubleType
taxi_data_2 = taxi_data \
              .withColumn("Trip_Distance",
                          taxi_data.Trip_Distance.cast(DoubleType()))
taxi_data_2.printSchema()
mtd_by_payment = taxi_data_2 \
                 .groupBy("Payment_Type") \
                 .avg("Trip_Distance")
mtd_by_payment.printSchema()
mtd_by_payment.show()

##############
#
# SQL on dataFrames
#
##############

taxi_data_2.createTempView("rides")
spark.sql("""select payment_type, avg(Trip_Distance) as avg_dist
	       from rides
	       group by payment_type""").show()


##############
#
# using in pandas
#
##############

import pandas as pd
pd_mtd = mtd_by_payment.toPandas()
type(pd_mtd)
pd_mtd