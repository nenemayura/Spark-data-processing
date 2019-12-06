from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, unix_timestamp
from pyspark.sql.types import TimestampType, BooleanType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

spark = SparkSession.builder.appName("my app").enableHiveSupport().getOrCreate()

# Q1
flight_data_orc2 = spark.table("flights.flight_data_orc2")
flightDF = flight_data_orc2.groupBy(flight_data_orc2.origin).agg({"dep_delay": "avg"}).withColumnRenamed(
    "avg(dep_delay)", "avg_dep_delay")
airport_codes = spark.table("flights.airport_codes")
airport = flightDF.sort(flightDF.avg_dep_delay.desc()).collect()[0][0]
airport_codes.filter(airport_codes.Code == airport).show()

#Q2
flight_data_denorm = spark.table("flights.flight_data_denorm")
df = flight_data_denorm.groupBy(flight_data_denorm.AIRPORT_NAMES.SOURCE).agg({"dep_delay": "avg"})
df = df.withColumnRenamed("AIRPORT_NAMES['SOURCE']", "AIRPORT_NAME").withColumnRenamed("avg(dep_delay)",
                                                                                       "AVG_DEP_DELAY")
df.sort(df.AVG_DEP_DELAY.desc()).collect()[0][0]

# Q3
flight_data_date = spark.table("flights.flight_data_date")
df = flight_data_date.groupBy(flight_data_date.carrier).agg({"arr_delay": "sum"})
df = df.withColumnRenamed("sum(arr_delay)", "arr_delay")
carrier = df.sort(df.arr_delay.desc()).collect()[0][0]
carrier_codes = spark.table("flights.carrier_codes")
carrier_codes.filter(carrier_codes.Code == carrier).show()

#Q4
flight_data_denorm = spark.table("flights.flight_data_denorm")
df = flight_data_denorm.groupBy(flight_data_denorm.CARRIER_NAME).agg({"ARR_DELAY": "sum"})
df = df.withColumnRenamed("sum(ARR_DELAY)", "ARR_DELAY")
df.sort(df.ARR_DELAY.desc()).collect()[0][0]
df.write.csv('q4.csv')

# Q8
flight_data_denorm = spark.table("flights.flight_data_denorm")


@F.udf(returnType=BooleanType())
def dateFilter(date):
    start_date = '12/20/2016'
    formatted_start_date = datetime.strptime(start_date, '%m/%d/%Y')
    return date > formatted_start_date


df = flight_data_denorm.filter(dateFilter(flight_data_denorm.FL_DATE))
df = df.groupBy(flight_data_denorm.AIRPORT_NAMES.DESTINATION).count()
df = df.withColumnRenamed("AIRPORT_NAMES['DESTINATION']", "AIRPORT_NAME").withColumnRenamed("count", "FLIGHT_COUNT")
df = df.sort(df.FLIGHT_COUNT.desc()).limit(3)
df.show()


# Q9
@F.udf(returnType=BooleanType())
def myFilter(dest):
    return dest.split(':')[0].split(',')[1] == ' MT'


df = flight_data_denorm.filter(myFilter(flight_data_denorm.AIRPORT_NAMES.DESTINATION))
partition_def = Window.partitionBy(df.AIRPORT_NAMES.DESTINATION)
df = df.withColumn("FLIGHT_COUNT", F.count("*").over(partition_def))
df = df.sort(df.FLIGHT_COUNT.desc()).limit(1)
df = df.select(df.DEST, df.FLIGHT_COUNT)
df.show()

# Q10
df = spark.sql("SELECT COUNT(*) FROM flights.flight_data_denorm")
df.show()
