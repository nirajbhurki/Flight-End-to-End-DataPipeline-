import dlt 
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Booking Data

@dlt.table(
    name = "Stage_bookings"
)

def stage_bookings():
    df = spark.readStream.format("delta")\
         .load("/Volumes/workspace/bronze/bronzevolume/bookings/data/")
    return df

@dlt.view(
    name = "trans_bookings"
)

def trans_bookings():
    df = spark.readStream.table("Stage_bookings")
    df = df.withColumn("amount",col("amount").cast(DoubleType()))\
         .withColumn("ModifiedDate",current_timestamp())\
         .withColumn("booking_date",to_date(col("booking_date")))\
         .drop("_rescued_data")
    return df
rules = {
    "rule1" : "booking_id IS NOT NULL",
    "rule2" : "passenger_id IS NOT NULL"

}

@dlt.table(
    name = "silver_bookings"
)
@dlt.expect_all_or_drop(rules)
def silver_bookings():
    df = spark.readStream.table("trans_bookings")
    return df


###########################################################################
# Flights Data

@dlt.view(
    name = "trans_flights"
)
def trans_flights():
    df = spark.readStream.format("delta")\
         .load("/Volumes/workspace/bronze/bronzevolume/flights/data/")
    df = df.withColumn("ModifiedDate", current_timestamp()) \
      .drop("_rescued_data")

    return df

dlt.create_streaming_table("silver_flights")

dlt.create_auto_cdc_flow(
    source = "trans_flights",
    target = "silver_flights",
    keys = ["flight_id"],
    sequence_by = col("ModifiedDate"),
    stored_as_scd_type = 1
)

#################################################################################
# Passengers Data

@dlt.view(
    name = "trans_passengers"
)
def trans_passengers():
    df = spark.readStream.format("delta")\
         .load("/Volumes/workspace/bronze/bronzevolume/customers/data/")
    df = df.withColumn("ModifiedDate", current_timestamp()) \
      .drop("_rescued_data")

    return df

dlt.create_streaming_table("silver_passengers")

dlt.create_auto_cdc_flow(
    source = "trans_passengers",
    target = "silver_passengers",
    keys = ["passenger_id"],
    sequence_by = col("ModifiedDate"),
    stored_as_scd_type = 1
)

############################################################################
# Airports Data
@dlt.view(
    name = "trans_airports"
)
def trans_airports():
    df = spark.readStream.format("delta")\
         .load("/Volumes/workspace/bronze/bronzevolume/airports/data/")
    df = df.withColumn("ModifiedDate", current_timestamp()) \
      .drop("_rescued_data")
      
    return df

dlt.create_streaming_table("silver_airports")

dlt.create_auto_cdc_flow(
    source = "trans_airports",
    target = "silver_airports",
    keys = ["airport_id"],
    sequence_by = col("ModifiedDate"),
    stored_as_scd_type = 1
)


####################################################################

# Silver Bussiness View

@dlt.table(
    name = "silver_bussiness_view"
)
def silver_bussiness_view():
    df = spark.readStream.table("silver_bookings")\
        .join(spark.readStream.table("silver_flights"), on = "flight_id")\
         .join(spark.readStream.table("silver_passengers"), on = "passenger_id")\
         .join(spark.readStream.table("silver_airports"), on = "airport_id")\
         .drop("modifiedDate")
    return df