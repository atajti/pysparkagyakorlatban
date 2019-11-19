#--------------------------------------------------------------------#
#                                                                    #
#                        PySpark RDDs                                #
#                                                                    #
#--------------------------------------------------------------------#
#
# Taxi ride data can be downloaded from https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
# Data sits on HDFS's /tmp/taxi_rides folder.


# hdfs dfs -ls -h /tmp/taxi_rides
# hdfs dfs -cat /tmp/taxi_rides/yellow_tripdata_2009-04.csv | head

##############
#
# pyspark
#
##############


read_04 = spark.sparkContext.textFile("/tmp/taxi_rides/yellow_tripdata_2009-04.csv")
read_04.getNumPartitions()

read_04.count()

# hdfs dfs -cat /tmp/taxi_rides/yellow_tripdata_2009-04.csv | wc -l

first_row = read_04.first()
first_row
# hdfs dfs -cat /tmp/taxi_rides/yellow_tripdata_2009-04.csv | head

from pprint import pprint
pprint(first_row.split(","))

first_rows = read_04.take(3)
pprint(first_rows)

pprint([row.split(",") for row in first_rows])
sample_data = [row.split(",") for row in first_rows][0:3:2]
[row[4] for row in sample_data]


##############
#
# Mean distance
#
##############

read_04_noempty = read_04\
                  .filter(lambda row: len(row) != 0) \
                  .filter(lambda row: row[0:11] != "vendor_name")
read_04_noempty.count()

mean_trip_distance = read_04_noempty \
                     .map(lambda row: row.split(",")[4]) \
                     .map(lambda x: float(x)).mean()
mean_trip_distance

##############
#
# Mean distance by payment type
#
##############

mtd_by_payment = read_04_noempty \
                 .map(lambda row: tuple([row.split(",")[11],
                                         (row.split(",")[4], 1)]))
pprint(mtd_by_payment.take(6))


summary_by_payment = mtd_by_payment \
                     .reduceByKey(lambda x,y: (float(x[0])+float(y[0]),
                                               x[1]+y[1]))
mtd_summary = summary_by_payment \
              .map(lambda row: (row[0],
                                row[1][0]/row[1][1]))

pprint(mtd_summary.collect())


##############
#
# extra depth: function for partitions, and to combine partitions
#
##############

mtd_by_payment = mtd_by_payment.aggregateByKey(("0.0", 0), lambda a,b: (float(a[0]) + float(b),
                                                                      a[1] + 1),
                                               lambda a,b: (a[0] + b[0], a[1] + b[1]))
mtd_by_payment.take(6)

mtd_by_payment_local = mtd_by_payment.mapValues(lambda v: v[0]/v[1]).collect()
pprint(mtd_by_payment_local)
