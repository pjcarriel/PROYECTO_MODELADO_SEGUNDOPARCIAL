"""
Persona 2 - Reglas de calidad de negocio para el pipeline ETL NYC TLC.
Implementa apply_quality_rules(df, process_id) y build_quality_metrics(...).
"""
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, year as spark_year,
    count, round as spark_round, row_number,
)
from pyspark.sql.types import LongType, DoubleType
from pyspark.sql.window import Window


# Definicion de las 13 reglas de calidad
_RULES = [
    {
        "rule": "NULL_CRITICAL_PICKUP",
        "column": "pickup_datetime",
        "condition": lambda: col("pickup_datetime").isNull(),
        "technical_reason": "pickup_datetime es NULL; campo critico obligatorio para calcular duracion y particion",
        "business_reason": "Sin fecha de inicio del viaje no es posible facturar ni clasificar el viaje por periodo",
    },
    {
        "rule": "NULL_CRITICAL_DROPOFF",
        "column": "dropoff_datetime",
        "condition": lambda: col("dropoff_datetime").isNull(),
        "technical_reason": "dropoff_datetime es NULL; campo critico para calcular trip_duration_minutes",
        "business_reason": "Sin fecha de fin del viaje no se puede calcular la duracion ni verificar la tarifa",
    },
    {
        "rule": "NULL_CRITICAL_DISTANCE",
        "column": "trip_distance",
        "condition": lambda: col("trip_distance").isNull(),
        "technical_reason": "trip_distance es NULL; campo critico para validar tarifas y velocidades",
        "business_reason": "Sin distancia del viaje no se puede verificar si la tarifa cobrada es razonable",
    },
    {
        "rule": "NULL_CRITICAL_FARE",
        "column": "fare_amount",
        "condition": lambda: col("fare_amount").isNull(),
        "technical_reason": "fare_amount es NULL; campo financiero critico",
        "business_reason": "Sin tarifa base no se puede calcular el revenue ni los KPIs financieros del servicio",
    },
    {
        "rule": "INVALID_DATE_RANGE",
        "column": "pickup_datetime",
        "condition": lambda: (spark_year(col("pickup_datetime")) < 2019) | (spark_year(col("pickup_datetime")) > 2024),
        "technical_reason": "Anio de pickup_datetime fuera del rango esperado 2019-2024",
        "business_reason": "Los datos NYC TLC del proyecto cubren 2019-2024; fechas fuera de rango son errores de captura",
    },
    {
        "rule": "NEGATIVE_AMOUNT",
        "column": "total_amount",
        "condition": lambda: col("total_amount") < 0,
        "technical_reason": "total_amount es negativo",
        "business_reason": "Una tarifa total negativa indica error de sistema o posible fraude; no debe procesarse",
    },
    {
        "rule": "NEGATIVE_FARE",
        "column": "fare_amount",
        "condition": lambda: col("fare_amount") < 0,
        "technical_reason": "fare_amount es negativo",
        "business_reason": "Una tarifa base negativa es imposible en el sistema de tarifas NYC TLC",
    },
    {
        "rule": "ZERO_DISTANCE",
        "column": "trip_distance",
        "condition": lambda: col("trip_distance") <= 0,
        "technical_reason": "trip_distance es 0 o negativo",
        "business_reason": "Un viaje con distancia cero o negativa no puede generar tarifa valida",
    },
    {
        "rule": "INVALID_DURATION",
        "column": "trip_duration_minutes",
        "condition": lambda: (col("trip_duration_minutes") <= 0) | (col("trip_duration_minutes") > 480),
        "technical_reason": "trip_duration_minutes fuera del rango valido (0, 480]",
        "business_reason": "Viajes de 0 minutos son imposibles; viajes de mas de 8 horas son anomalos en NYC",
    },
    {
        "rule": "UNREALISTIC_SPEED",
        "column": "average_speed_mph",
        "condition": lambda: col("average_speed_mph") > 100,
        "technical_reason": "average_speed_mph > 100 mph en trafico urbano NYC",
        "business_reason": "Una velocidad superior a 100 mph es fisicamente imposible en el trafico de Nueva York",
    },
    {
        "rule": "FUTURE_DATE",
        "column": "pickup_datetime",
        "condition": lambda: col("pickup_datetime") > current_timestamp(),
        "technical_reason": "pickup_datetime es posterior al timestamp actual de procesamiento",
        "business_reason": "Un viaje con fecha futura no puede haber ocurrido; es un error de ingesta",
    },
    {
        "rule": "INVERTED_DATES",
        "column": "pickup_datetime",
        "condition": lambda: col("pickup_datetime") > col("dropoff_datetime"),
        "technical_reason": "pickup_datetime es posterior a dropoff_datetime (viaje invertido)",
        "business_reason": "El inicio del viaje no puede ser posterior al fin; indica corrupcion de datos",
    },
    {
        "rule": "DUPLICATE_TRIP",
        "column": "trip_id",
        "condition": None,  # Manejado con Window function
        "technical_reason": "trip_id aparece mas de una vez en el dataset (duplicado tecnico)",
        "business_reason": "Registros duplicados inflan las metricas de revenue y conteo de viajes",
    },
]


def _build_rejected_rows(src_df, rule_def, process_id: str) -> DataFrame:
    """Genera el DataFrame de registros rechazados para una regla especifica."""
    col_name = rule_def["column"]
    orig_val = col(col_name).cast("string") if col_name in src_df.columns else lit(None).cast("string")
    return src_df.select(
        lit(process_id).alias("process_id"),
        col("trip_id"),
        col("service_type"),
        col("source_file"),
        lit("QUALITY").alias("rejection_stage"),
        lit(rule_def["rule"]).alias("rejection_rule"),
        lit(col_name).alias("rejection_column"),
        orig_val.alias("original_value"),
        lit(rule_def["technical_reason"]).alias("technical_reason"),
        lit(rule_def["business_reason"]).alias("business_reason"),
        current_timestamp().alias("rejected_at"),
    )


def apply_quality_rules(df, process_id: str):
    """
    Evalua cada registro contra las 13 reglas de calidad.

    Retorna:
        (df_valid, df_rejected_records)
        - df_valid: registros que pasan todas las reglas con quality_status = 'VALID'
        - df_rejected_records: tabla quality_rejected_records con 11 campos,
          un registro por cada violacion (un viaje puede generar multiples filas)
    """
    rejected_parts = []
    invalid_condition = lit(False)

    for rule_def in _RULES:
        if rule_def["rule"] == "DUPLICATE_TRIP":
            # Detectar duplicados con Window: conservar solo la primera ocurrencia
            w_dup = Window.partitionBy("trip_id").orderBy("ingestion_timestamp")
            df_ranked = df.withColumn("_dup_rank", row_number().over(w_dup))
            dup_df = df_ranked.filter(col("_dup_rank") > 1).drop("_dup_rank")
            rejected_parts.append(_build_rejected_rows(dup_df, rule_def, process_id))
        else:
            cond = rule_def["condition"]()
            rejected_parts.append(_build_rejected_rows(df.filter(cond), rule_def, process_id))
            invalid_condition = invalid_condition | cond

    # Registros validos: pasan todas las reglas Y son la primera ocurrencia de su trip_id
    w_valid = Window.partitionBy("trip_id").orderBy("ingestion_timestamp")
    df_valid = (
        df.withColumn("_dup_rank", row_number().over(w_valid))
        .filter(~invalid_condition & (col("_dup_rank") == 1))
        .drop("_dup_rank")
        .withColumn("quality_status", lit("VALID"))
    )

    df_rejected = reduce(DataFrame.union, rejected_parts)
    return df_valid, df_rejected


def build_quality_metrics(df_transformed, df_valid, df_rejected, process_id: str) -> DataFrame:
    """
    Construye quality_metrics_summary agrupado por service_type/year/month.

    Args:
        df_transformed: DataFrame DESPUES de apply_transformations (antes de quality filter).
                        Debe tener columnas is_suspicious_trip, year, month.
        df_valid:       DataFrame de registros validos (resultado de apply_quality_rules).
        df_rejected:    DataFrame quality_rejected_records (resultado de apply_quality_rules).
        process_id:     UUID de la ejecucion actual.
    """
    grp = ["service_type", "year", "month"]

    total_df = df_transformed.groupBy(*grp).agg(
        count("*").cast(LongType()).alias("total_records")
    )
    valid_df = df_valid.groupBy(*grp).agg(
        count("*").cast(LongType()).alias("valid_records")
    )
    suspicious_df = (
        df_transformed.filter(col("is_suspicious_trip"))
        .groupBy(*grp)
        .agg(count("*").cast(LongType()).alias("suspicious_records"))
    )
    null_critical_df = (
        df_transformed.filter(
            col("pickup_datetime").isNull()
            | col("dropoff_datetime").isNull()
            | col("trip_distance").isNull()
            | col("fare_amount").isNull()
        )
        .groupBy(*grp)
        .agg(count("*").cast(LongType()).alias("null_critical_records"))
    )

    # Duplicados: los que tienen la misma trip_id y no son la primera ocurrencia
    w_dup = Window.partitionBy("trip_id").orderBy("ingestion_timestamp")
    duplicate_df = (
        df_transformed.withColumn("_dup_rank", row_number().over(w_dup))
        .filter(col("_dup_rank") > 1)
        .groupBy(*grp)
        .agg(count("*").cast(LongType()).alias("duplicate_records"))
    )

    metrics = (
        total_df
        .join(valid_df, grp, "left")
        .join(suspicious_df, grp, "left")
        .join(null_critical_df, grp, "left")
        .join(duplicate_df, grp, "left")
        .fillna(0, subset=["valid_records", "suspicious_records", "null_critical_records", "duplicate_records"])
    )

    return metrics.select(
        lit(process_id).alias("process_id"),
        col("service_type"),
        col("year"),
        col("month"),
        col("total_records"),
        col("valid_records").cast(LongType()),
        (col("total_records") - col("valid_records")).cast(LongType()).alias("rejected_records"),
        col("duplicate_records").cast(LongType()),
        col("null_critical_records").cast(LongType()),
        col("suspicious_records").cast(LongType()),
        spark_round((col("valid_records") / col("total_records")) * 100.0, 2)
            .cast(DoubleType()).alias("quality_percentage"),
        current_timestamp().alias("processed_at"),
    )
