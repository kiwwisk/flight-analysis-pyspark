import json
from datetime import datetime
from functools import lru_cache

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F


def _load_table(spark, table):
    df = (spark.read
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/zadanie")
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", table)
        .option("user", "postgres")
        .option("password", "postgres")
        .load()
    )
    return df


def _write_table(df, table):
    (df.write
      .format("jdbc")
      .option("url", "jdbc:postgresql://postgres:5432/zadanie")
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", table)
      .option("user", "postgres")
      .option("password", "postgres")
      .mode("overwrite")
      .save())


@lru_cache
def get_spark_session():
    spark = (SparkSession.builder
        .appName("ZadanieValllueJob")
        .master("spark://spark-master:7077")
        .config("spark.jars", "/opt/postgres/postgresql-42.7.4.jar")
        .getOrCreate())
    return spark


def task_1():
    spark = get_spark_session()

    with open('oag_multiple.json', 'r') as f_in:
        oag_data = json.load(f_in)

    oag_rdd = spark.sparkContext.parallelize(oag_data['data'])
    df = spark.read.json(oag_rdd)

    # select most current status item (grouped by statusKey)
    w = Window.partitionBy("statusKey").orderBy(F.desc(F.element_at(F.col("statusDetails.updatedAt"), -1)))
    current = (df
           .withColumn("row_number_in_group", F.row_number().over(w))
           .filter(F.col("row_number_in_group")==1)
           .drop("row_number_in_group"))

    out = current.select(
        # transform required columns to datetimes
        F.to_timestamp(
            F.concat(F.col("departure.date.utc"), F.lit(" "), F.col("departure.time.utc"))
        ).alias("planned_departure"),

        F.to_timestamp(
            F.concat(F.col("arrival.date.utc"), F.lit(" "), F.col("arrival.time.utc"))
        ).alias("planned_arrival"),

        # NOTE: select last item from statusDetails (last known status)
        F.to_timestamp(
            F.element_at(F.col("statusDetails.departure.actualTime.outGate.utc"), -1)
        ).alias("departure_out_gate"),

        F.to_timestamp(
            F.element_at(F.col("statusDetails.arrival.actualTime.inGate.utc"), -1)
        ).alias("arrival_in_gate"),

        F.to_timestamp(
            F.element_at(F.col("statusDetails.departure.actualTime.offGround.utc"), -1)
        ).alias("departure_off_ground"),

        F.to_timestamp(
            F.element_at(F.col("statusDetails.arrival.actualTime.onGround.utc"), -1)
        ).alias("arrival_on_ground"),
    ).select(
        # some flights don't have in/out gate times, so use off/on ground times in that case
        F.col("planned_departure"),
        F.col("planned_arrival"),
        F.coalesce(F.col("departure_out_gate"), F.col("departure_off_ground")).alias("departure"),
        F.coalesce(F.col("arrival_in_gate"), F.col("arrival_on_ground")).alias("arrival"),
    ).select(
        # count number of delayed departures/arrivals
        F.sum(F.when(F.col("planned_departure") < F.col("departure"), 1)).alias("departure delays"),
        F.sum(F.when(F.col("planned_arrival") < F.col("arrival"), 1)).alias("arrival delays"),
    )

    out.show(truncate=False)

def task_2():
    spark = get_spark_session()

    # save adsb data as is
    with open('adsb_multi_aircraft.json', 'r') as f_in:
        adsb_data = json.load(f_in)

    adsb_rdd = spark.sparkContext.parallelize(adsb_data)
    df_adsb = spark.read.json(adsb_rdd)

    _write_table(df_adsb, "adsb_flightradar24")

    # transform and save oag data
    with open('oag_multiple.json', 'r') as f_in:
        oag_data = json.load(f_in)

    oag_rdd = spark.sparkContext.parallelize(oag_data["data"])
    df = spark.read.json(oag_rdd)

    # create oag_schedules table:

    df_schedule = df.select(
        F.col("statusKey").alias("statuskey"),
        F.col("carrier.iata").alias("carrier_iata"),
        F.col("carrier.icao").alias("carrier_icao"),
        F.col("flightNumber").alias("flightnumber"),
        F.col("departure.airport.iata").alias("departure_airport_iata"),
        F.col("departure.airport.icao").alias("departure_airport_icao"),
        F.col("departure.date.utc").alias("departure_date_utc"),
        F.col("departure.time.utc").alias("departure_time_utc"),
        F.col("arrival.date.utc").alias("arrival_date_utc"),
        F.col("arrival.time.utc").alias("arrival_time_utc"),
    )

    _write_table(df_schedule, "oag_schedules")

    # create oag_statuses from selected fields

    df_statuses = (df.select(
            F.col("statusKey").alias("statuskey"),
            F.col("statusDetails.state").alias("state"),
            F.col("statusDetails.updatedAt").alias("updatedat"),
            F.col("statusDetails.equipment.aircraftRegistrationNumber").alias("aircraftregnumber"),
        ).withColumn("tmp", F.arrays_zip("state", "updatedat", "aircraftregnumber"))
        .withColumn("tmp", F.explode("tmp"))
        .select("statuskey",
            F.col("tmp.state").alias("state"),
            F.col("tmp.updatedat").alias("updatedat"),
            F.col("tmp.aircraftregnumber").alias("aircraftregnumber"),

        )
    )

    _write_table(df_statuses, "oag_statuses")

    # load from SQL tables back
    df1 = _load_table(spark, "oag_statuses")
    df2 = _load_table(spark, "oag_schedules")

    # do sample join by statuskey and add flight number to each status
    (df1
        .join(
            df2.select(
                F.col("statuskey"),
                F.col("flightnumber")
            ), on="statuskey", how="left")
        .show(truncate=False))

if __name__ == "__main__":
    task_1()
    task_2()
