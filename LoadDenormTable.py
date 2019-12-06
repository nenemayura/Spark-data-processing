from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("my app").enableHiveSupport().getOrCreate()

airport_codes = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/root/codes_data/airport/L_AIRPORT.csv")
carrier_codes = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/root/codes_data/carrier_history/L_CARRIER_HISTORY.csv")

#Filtering
def inRange(s):
    i=s.rfind("(")
    j=s.rfind(")")
    r = s[i+1:j]
    ii=r.split('-')
    if len(ii[1])<5:
            return int(ii[0]) <= 2016
    else:
            return int(ii[0]) <= 2016 and int(ii[1]) > 2016
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
carrier_codes = carrier_codes.filter(udf(lambda target: inRange(target),  BooleanType())(carrier_codes.Description))

carrier_codes.write.format("orc").mode("overwrite").saveAsTable("flights.carrier_codes")
airport_codes.write.format("orc").mode("overwrite").saveAsTable("flights.airport_codes")

flight_data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/root/flight_data/*.csv")
flight_airport_join = flight_data.join(airport_codes, flight_data.ORIGIN == airport_codes.Code)
flight_airport_join = flight_airport_join.drop(flight_airport_join.Code).withColumnRenamed("Description", "SOURCE")
flight_airport_join = flight_airport_join.join(airport_codes, flight_airport_join.DEST == airport_codes.Code)
flight_airport_join  = flight_airport_join .drop(flight_airport_join.Code).withColumnRenamed("Description", "DESTINATION")
flight_airport_join.select(flight_airport_join.ORIGIN, flight_airport_join.SOURCE, flight_airport_join.DEST, flight_airport_join.DESTINATION).show(5)
flight_airport__carrier_join = flight_airport_join.join(carrier_codes, flight_airport_join.CARRIER == carrier_codes.Code)
flight_airport__carrier_join = flight_airport__carrier_join.drop(flight_airport__carrier_join.Code).withColumnRenamed("Description", "CARRIER_NAME")
from pyspark.sql import functions as f
flight_data_denorm =flight_airport__carrier_join.withColumn("AIRPORT_NAMES", f.struct(flight_airport__carrier_join.SOURCE, flight_airport__carrier_join.DESTINATION))
flight_data_denorm.write.format("orc").mode("overwrite").saveAsTable("flights.flight_data_denorm")
