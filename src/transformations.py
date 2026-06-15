"""
Persona 2 — Transformaciones avanzadas con Apache Spark.
Implementa apply_transformations(df) con las 11 transformaciones obligatorias
más el campo is_suspicious_trip y processing_date.
"""
from pyspark.sql.functions import (
    col, unix_timestamp, when, lit,
    round as spark_round, current_timestamp, date_trunc,
)
from pyspark.sql.types import TimestampType, DoubleType, DateType


def apply_transformations(df):
    """
    Aplica todas las transformaciones obligatorias al DataFrame canónico (bronze).
    Retorna el DataFrame transformado y listo para escribir en silver.

    Transformaciones aplicadas (en orden):
      1.  Normalización de nombres de columnas a snake_case
      2.  Conversión de fechas a TimestampType (null on fail)
      3.  trip_duration_minutes
      4.  average_speed_mph
      5.  fare_per_mile
      6.  tip_percentage
      7.  is_airport_trip
      8.  Redondeo de montos a 2 decimales
      9.  is_suspicious_trip
      10. Eliminación de duplicados técnicos por trip_id
      11. processing_date
    """
    # 1. Normalizar nombres de columnas a snake_case
    for old_name in df.columns:
        new_name = old_name.lower().replace(" ", "_")
        if new_name != old_name:
            df = df.withColumnRenamed(old_name, new_name)

    # 2. Conversión de fechas a TimestampType
    #    El cast nativo de Spark produce null si el valor no es parseable.
    df = df.withColumn("pickup_datetime", col("pickup_datetime").cast(TimestampType()))
    df = df.withColumn("dropoff_datetime", col("dropoff_datetime").cast(TimestampType()))

    # 3. trip_duration_minutes = (dropoff - pickup) / 60 segundos
    df = df.withColumn(
        "trip_duration_minutes",
        when(
            col("pickup_datetime").isNotNull() & col("dropoff_datetime").isNotNull(),
            (unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")) / 60.0,
        ).otherwise(lit(None).cast(DoubleType())),
    )

    # 4. average_speed_mph = distance / (duration_minutes / 60)
    df = df.withColumn(
        "average_speed_mph",
        when(
            (col("trip_duration_minutes") > 0) & (col("trip_distance") > 0),
            col("trip_distance") / (col("trip_duration_minutes") / 60.0),
        ).otherwise(lit(None).cast(DoubleType())),
    )

    # 5. fare_per_mile = fare_amount / trip_distance
    df = df.withColumn(
        "fare_per_mile",
        when(
            col("trip_distance") > 0,
            col("fare_amount") / col("trip_distance"),
        ).otherwise(lit(None).cast(DoubleType())),
    )

    # 6. tip_percentage = (tip_amount / fare_amount) * 100
    df = df.withColumn(
        "tip_percentage",
        when(
            col("fare_amount") > 0,
            (col("tip_amount") / col("fare_amount")) * 100.0,
        ).otherwise(lit(None).cast(DoubleType())),
    )

    # 7. is_airport_trip: location IDs 1=Newark, 132=JFK, 138=LaGuardia
    airport_ids = [1, 132, 138]
    df = df.withColumn(
        "is_airport_trip",
        col("pickup_location_id").isin(airport_ids) | col("dropoff_location_id").isin(airport_ids),
    )

    # 8. Normalización de montos monetarios a 2 decimales
    monetary_cols = [
        "fare_amount", "tip_amount", "total_amount", "extra_amount",
        "tolls_amount", "congestion_surcharge", "airport_fee", "improvement_surcharge",
    ]
    for mc in monetary_cols:
        if mc in df.columns:
            df = df.withColumn(mc, spark_round(col(mc), 2))

    # 9. is_suspicious_trip: True si cumple UNO O MAS de los criterios del enunciado.
    # Las condiciones .isNull() se agregan primero para capturar campos criticos ausentes,
    # ya que comparaciones aritmeticas con null devuelven null (no True) en Spark.
    is_suspicious = (
        col("trip_distance").isNull()
        | col("fare_amount").isNull()
        | col("pickup_datetime").isNull()
        | col("dropoff_datetime").isNull()
        | (col("trip_distance") <= 0)
        | (col("total_amount") <= 0)
        | (col("fare_amount") < 0)
        | (col("trip_duration_minutes") <= 0)
        | (col("trip_duration_minutes") > 480)
        | (col("average_speed_mph") > 100)
        | (col("tip_percentage") > 100)
        | (col("pickup_datetime") > col("dropoff_datetime"))
        | (col("pickup_datetime") > current_timestamp())
    )
    df = df.withColumn("is_suspicious_trip", when(is_suspicious, True).otherwise(False))

    # 10. Eliminacion de duplicados tecnicos por trip_id
    df = df.dropDuplicates(["trip_id"])

    # 11. processing_date = fecha actual truncada al dia
    df = df.withColumn(
        "processing_date",
        date_trunc("day", current_timestamp()).cast(DateType()),
    )

    # Optimizacion #5: coalesce para evitar miles de archivos pequenos al escribir silver
    return df.coalesce(4)
