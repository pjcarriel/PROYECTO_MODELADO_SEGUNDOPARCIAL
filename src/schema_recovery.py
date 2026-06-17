import json
import os
from pathlib import Path
from datetime import datetime, timezone

from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from utils import resolve_path

SCHEMA_DIFF_SCHEMA = StructType([
    StructField("process_id", StringType(), False),
    StructField("service_type", StringType(), True),
    StructField("file_name", StringType(), True),
    StructField("schema_hash", StringType(), True),
    StructField("column_name", StringType(), True),
    StructField("diff_type", StringType(), True),
    StructField("expected_type", StringType(), True),
    StructField("actual_type", StringType(), True),
    StructField("diagnostic_at", TimestampType(), True),
])

def load_expected_schema(metadata_dir: str, service_type: str) -> dict:
    path = Path(metadata_dir) / f"expected_schema_{service_type}.json"
    if not path.exists():
        raise FileNotFoundError(f"No existe esquema esperado: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def expected_fields_map(expected_schema: dict) -> dict:
    return {f["name"]: f["type"] for f in expected_schema["fields"]}

def spark_schema_map(df) -> dict:
    """
    Convierte el schema real de Spark a diccionario nombre -> tipo simpleString.
    """
    return {field.name: field.dataType.simpleString() for field in df.schema.fields}

def compare_schema_maps(expected: dict, actual: dict) -> list:
    """
    Genera diferencias:
    - EXTRA_COLUMN
    - MISSING_COLUMN
    - TYPE_MISMATCH
    """
    diffs = []
    for col, exp_type in expected.items():
        if col not in actual:
            diffs.append({
                "column_name": col,
                "diff_type": "MISSING_COLUMN",
                "expected_type": exp_type,
                "actual_type": None,
            })
        else:
            act_type = actual[col]
            # Comparación flexible básica para evitar falsos positivos entre bigint/long o int/integer
            norm_exp = normalize_type(exp_type)
            norm_act = normalize_type(act_type)
            if norm_exp != norm_act:
                diffs.append({
                    "column_name": col,
                    "diff_type": "TYPE_MISMATCH",
                    "expected_type": exp_type,
                    "actual_type": act_type,
                })

    for col, act_type in actual.items():
        if col not in expected:
            diffs.append({
                "column_name": col,
                "diff_type": "EXTRA_COLUMN",
                "expected_type": None,
                "actual_type": act_type,
            })
    return diffs

def normalize_type(t):
    if t is None:
        return None
    t = str(t).lower()
    aliases = {
        "bigint": "long",
        "integer": "int",
        "timestamp_ntz": "timestamp",
        "decimal(38,18)": "decimal",
        "decimal(10,2)": "decimal",
    }
    return aliases.get(t, t)

def diagnose_successful_files(spark, config: dict, inventory_df, process_id: str):
    """
    Lee los archivos con SUCCESS y genera matriz de diferencias de esquema.
    Usa pyarrow para leer el schema (evita getSubject de Java 17/18+).
    """
    import pyarrow.parquet as pq

    # Mapeo de tipos pyarrow → tipos Spark simpleString
    _PA_TO_SPARK = {
        "int8": "tinyint", "int16": "smallint", "int32": "int", "int64": "long",
        "uint8": "int", "uint16": "int", "uint32": "long", "uint64": "long",
        "float": "float", "double": "double", "float16": "float",
        "bool": "boolean", "large_binary": "binary", "binary": "binary",
        "string": "string", "large_string": "string", "utf8": "string", "large_utf8": "string",
        "date32[day]": "date", "date64[ms]": "date",
    }

    def pa_type_to_spark(pa_type_str: str) -> str:
        if pa_type_str in _PA_TO_SPARK:
            return _PA_TO_SPARK[pa_type_str]
        if pa_type_str.startswith("timestamp"):
            return "timestamp"
        if pa_type_str.startswith("decimal"):
            return "decimal"
        return pa_type_str

    metadata_dir = resolve_path(config, "metadata")
    success_rows = (
        inventory_df
        .filter("read_status = 'SUCCESS' AND service_type IN ('yellow', 'green', 'fhvhv')")
        .select("service_type", "file_name", "file_path", "schema_hash")
        .collect()
    )

    rows = []
    diagnostic_at = datetime.now(timezone.utc).replace(tzinfo=None)
    for r in success_rows:
        expected = expected_fields_map(load_expected_schema(str(metadata_dir), r["service_type"]))
        with open(r["file_path"], "rb") as _fh:
            pa_schema = pq.read_schema(_fh)
        actual = {field.name: pa_type_to_spark(str(field.type)) for field in pa_schema}
        for diff in compare_schema_maps(expected, actual):
            rows.append({
                "process_id": process_id,
                "service_type": r["service_type"],
                "file_name": r["file_name"],
                "schema_hash": r["schema_hash"],
                "column_name": diff["column_name"],
                "diff_type": diff["diff_type"],
                "expected_type": diff["expected_type"],
                "actual_type": diff["actual_type"],
                "diagnostic_at": diagnostic_at,
            })

    return spark.createDataFrame(rows, schema=SCHEMA_DIFF_SCHEMA)

def write_schema_differences(df, config: dict):
    import shutil
    output_path = resolve_path(config, "audit") / "schema_differences"
    if output_path.exists():
        shutil.rmtree(str(output_path))
    output_path.mkdir(parents=True, exist_ok=True)
    df.toPandas().to_parquet(str(output_path / "part-00000.parquet"), index=False, engine="pyarrow")
    return str(output_path)


# ─────────────────────────────────────────────────────────────────────────────
# PERSONA 2 — Reconstrucción canónica y manejo de archivos dañados
# ─────────────────────────────────────────────────────────────────────────────

CANONICAL_FIELDS = [
    "trip_id", "service_type", "vendor_id", "pickup_datetime", "dropoff_datetime",
    "passenger_count", "trip_distance", "pickup_location_id", "dropoff_location_id",
    "payment_type", "fare_amount", "extra_amount", "mta_tax", "tip_amount",
    "tolls_amount", "total_amount", "congestion_surcharge", "airport_fee",
    "year", "month", "source_file", "ingestion_timestamp", "improvement_surcharge",
    "quality_status",
]


def _load_business_rules(config: dict) -> dict:
    rules_path = resolve_path(config, "metadata") / "business_rules.json"
    with rules_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def apply_canonical_schema(df, service_type: str, source_file: str, config: dict):
    """
    Homologa un DataFrame de cualquier servicio (yellow/green/fhvhv) al esquema
    canónico unificado de 24 campos definido en canonical_schema_trips.json.

    - Columnas ausentes se agregan como null del tipo correcto.
    - Tipos incorrectos se castean (Spark produce null si el cast falla).
    - Columnas extra no canónicas se descartan.
    """
    from pyspark.sql.functions import (
        col, lit, sha2, concat_ws, current_timestamp,
        year as spark_year, month as spark_month, coalesce,
    )
    from pyspark.sql.types import StringType, DoubleType, IntegerType, TimestampType

    rules = _load_business_rules(config)
    hom = rules["homologation"]

    def resolve_col(canonical_name, target_type):
        src_name = hom.get(canonical_name, {}).get(service_type)
        if src_name and src_name in df.columns:
            return col(src_name).cast(target_type)
        return lit(None).cast(target_type)

    # Columnas clave para trip_id
    pickup_src = hom.get("pickup_datetime", {}).get(service_type)
    dropoff_src = hom.get("dropoff_datetime", {}).get(service_type)
    vendor_src = hom.get("vendor_id", {}).get(service_type)
    fare_src = hom.get("fare_amount", {}).get(service_type)

    pickup_expr = col(pickup_src).cast(TimestampType()) if pickup_src and pickup_src in df.columns else lit(None).cast(TimestampType())
    dropoff_expr = col(dropoff_src).cast(TimestampType()) if dropoff_src and dropoff_src in df.columns else lit(None).cast(TimestampType())
    vendor_expr = col(vendor_src).cast(StringType()) if vendor_src and vendor_src in df.columns else lit(None).cast(StringType())
    fare_expr = col(fare_src).cast(DoubleType()) if fare_src and fare_src in df.columns else lit(None).cast(DoubleType())

    # Caso especial fhvhv: total_amount = base_passenger_fare + tolls + sales_tax
    if service_type == "fhvhv":
        bpf = col("base_passenger_fare").cast(DoubleType()) if "base_passenger_fare" in df.columns else lit(0.0)
        tolls_c = coalesce(col("tolls").cast(DoubleType()), lit(0.0)) if "tolls" in df.columns else lit(0.0)
        stax = coalesce(col("sales_tax").cast(DoubleType()), lit(0.0)) if "sales_tax" in df.columns else lit(0.0)
        total_expr = (bpf + tolls_c + stax).cast(DoubleType())
    else:
        total_expr = resolve_col("total_amount", DoubleType())

    # improvement_surcharge: columna directa en yellow/green, null en fhvhv
    impr_src = hom.get("improvement_surcharge", {}).get(service_type)
    impr_expr = col(impr_src).cast(DoubleType()) if impr_src and impr_src in df.columns else lit(None).cast(DoubleType())

    return df.select(
        sha2(concat_ws("|",
            lit(service_type),
            pickup_expr.cast(StringType()),
            dropoff_expr.cast(StringType()),
            vendor_expr,
            fare_expr.cast(StringType()),
        ), 256).alias("trip_id"),
        lit(service_type).cast(StringType()).alias("service_type"),
        vendor_expr.alias("vendor_id"),
        pickup_expr.alias("pickup_datetime"),
        dropoff_expr.alias("dropoff_datetime"),
        resolve_col("passenger_count", DoubleType()).alias("passenger_count"),
        resolve_col("trip_distance", DoubleType()).alias("trip_distance"),
        resolve_col("pickup_location_id", IntegerType()).alias("pickup_location_id"),
        resolve_col("dropoff_location_id", IntegerType()).alias("dropoff_location_id"),
        resolve_col("payment_type", StringType()).alias("payment_type"),
        fare_expr.alias("fare_amount"),
        resolve_col("extra_amount", DoubleType()).alias("extra_amount"),
        resolve_col("mta_tax", DoubleType()).alias("mta_tax"),
        resolve_col("tip_amount", DoubleType()).alias("tip_amount"),
        resolve_col("tolls_amount", DoubleType()).alias("tolls_amount"),
        total_expr.alias("total_amount"),
        resolve_col("congestion_surcharge", DoubleType()).alias("congestion_surcharge"),
        resolve_col("airport_fee", DoubleType()).alias("airport_fee"),
        spark_year(pickup_expr).alias("year"),
        spark_month(pickup_expr).alias("month"),
        lit(source_file).cast(StringType()).alias("source_file"),
        current_timestamp().alias("ingestion_timestamp"),
        impr_expr.alias("improvement_surcharge"),
        lit("PENDING").cast(StringType()).alias("quality_status"),
    )


def apply_canonical_schema_pd(df_pd, service_type: str, source_file: str, config: dict):
    """
    Versión pandas de apply_canonical_schema() — misma lógica sin Spark.
    Evita el lento spark.createDataFrame(pandas_df) para DataFrames de 3M+ filas.
    Retorna un pandas DataFrame con exactamente los 24 campos canónicos.
    """
    import hashlib
    import pandas as pd
    from datetime import datetime, timezone

    rules = _load_business_rules(config)
    hom = rules["homologation"]

    out = {}

    # Aplicar homologación: para cada campo canónico, buscar la columna origen del servicio
    for canon_col, mapping in hom.items():
        src = mapping.get(service_type)
        if src and src in df_pd.columns:
            out[canon_col] = df_pd[src].values.copy()
        else:
            out[canon_col] = None

    # Caso especial fhvhv: total_amount = base_passenger_fare + tolls + sales_tax
    if service_type == "fhvhv":
        bpf = pd.to_numeric(df_pd.get("base_passenger_fare", 0), errors="coerce").fillna(0)
        tl  = pd.to_numeric(df_pd.get("tolls", 0), errors="coerce").fillna(0)
        stx = pd.to_numeric(df_pd.get("sales_tax", 0), errors="coerce").fillna(0)
        out["total_amount"] = (bpf + tl + stx).values

    # improvement_surcharge: yellow/green tienen la columna, fhvhv no
    impr_src = hom.get("improvement_surcharge", {}).get(service_type)
    out["improvement_surcharge"] = df_pd[impr_src].values if impr_src and impr_src in df_pd.columns else None

    df_out = pd.DataFrame(out, index=range(len(df_pd)))

    # Castear tipos numéricos
    for col in ["fare_amount", "extra_amount", "mta_tax", "tip_amount", "tolls_amount",
                "total_amount", "congestion_surcharge", "airport_fee", "improvement_surcharge",
                "trip_distance", "passenger_count"]:
        if col in df_out.columns:
            df_out[col] = pd.to_numeric(df_out[col], errors="coerce")

    for col in ["pickup_location_id", "dropoff_location_id"]:
        if col in df_out.columns:
            df_out[col] = pd.to_numeric(df_out[col], errors="coerce")

    # Castear timestamps
    df_out["pickup_datetime"]  = pd.to_datetime(df_out.get("pickup_datetime"),  errors="coerce")
    df_out["dropoff_datetime"] = pd.to_datetime(df_out.get("dropoff_datetime"), errors="coerce")

    # Columnas de metadatos
    df_out["service_type"]        = service_type
    df_out["source_file"]         = source_file
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    df_out["ingestion_timestamp"] = now
    df_out["quality_status"]      = "PENDING"

    # year/month desde pickup_datetime
    df_out["year"]  = df_out["pickup_datetime"].dt.year.fillna(0).astype("Int64")
    df_out["month"] = df_out["pickup_datetime"].dt.month.fillna(0).astype("Int64")

    # trip_id: SHA256 idéntico al sha2(concat_ws(...), 256) de Spark
    key_series = (
        service_type + "|"
        + df_out["pickup_datetime"].astype(str) + "|"
        + df_out["dropoff_datetime"].astype(str) + "|"
        + df_out.get("vendor_id", pd.Series([""] * len(df_out))).fillna("").astype(str) + "|"
        + df_out.get("fare_amount", pd.Series([None] * len(df_out))).astype(str)
    )
    df_out["trip_id"] = key_series.apply(lambda s: hashlib.sha256(s.encode()).hexdigest())

    # Retornar solo los campos canónicos en el orden definido
    return df_out[[c for c in CANONICAL_FIELDS if c in df_out.columns]]


_QUARANTINE_ACTIONS = {
    "RECUPERABLE_SCHEMA_MISMATCH":         "Aplicar schema recovery con business_rules.json y re-intentar ingesta",
    "RECUPERABLE_MISSING_COLUMNS":         "Agregar columnas faltantes con valor null y re-intentar ingesta",
    "RECUPERABLE_TYPE_CASTING":            "Aplicar cast() a los tipos correctos y re-intentar ingesta",
    "PARTIALLY_RECOVERABLE":               "Extraer row groups válidos con lectura parcial y registrar la pérdida",
    "NOT_RECOVERABLE_CORRUPT_METADATA":    "Archivar en storage frío, solicitar re-descarga al origen",
    "NOT_RECOVERABLE_EMPTY_FILE":          "Eliminar y re-descargar desde la fuente NYC TLC",
    "NOT_RECOVERABLE_UNSUPPORTED_FORMAT":  "Verificar la fuente; el archivo no es un Parquet válido",
}


def classify_corrupt_file(file_path: str, error_msg: str) -> str:
    """
    Clasifica un archivo problemático en una de las 7 categorías de quarantine
    basándose en el texto del error y el tamaño del archivo.
    """
    error_lower = (error_msg or "").lower()

    try:
        if os.path.getsize(str(file_path)) == 0:
            return "NOT_RECOVERABLE_EMPTY_FILE"
    except (OSError, FileNotFoundError):
        pass

    if "empty" in error_lower or "0 bytes" in error_lower or "no rows" in error_lower:
        return "NOT_RECOVERABLE_EMPTY_FILE"
    if "footer" in error_lower or "magic" in error_lower or ("metadata" in error_lower and "corrupt" in error_lower):
        return "NOT_RECOVERABLE_CORRUPT_METADATA"
    if "partial" in error_lower or "row group" in error_lower:
        return "PARTIALLY_RECOVERABLE"
    if "schema" in error_lower or "mismatch" in error_lower:
        return "RECUPERABLE_SCHEMA_MISMATCH"
    if "missing" in error_lower or "not found" in error_lower or "column" in error_lower:
        return "RECUPERABLE_MISSING_COLUMNS"
    if "cast" in error_lower or "type" in error_lower or "convert" in error_lower:
        return "RECUPERABLE_TYPE_CASTING"
    return "NOT_RECOVERABLE_UNSUPPORTED_FORMAT"


def generate_quarantine_record(
    file_name: str,
    file_path: str,
    classification: str,
    exception_text: str,
    phase: str,
    quarantine_dir: str,
) -> dict:
    """
    Crea el registro JSON de quarantine con los 7 campos requeridos y lo escribe en disco.
    """
    record = {
        "file_name": file_name,
        "file_path": str(file_path),
        "classification": classification,
        "exception_text": exception_text,
        "processing_timestamp": datetime.now(timezone.utc).isoformat(),
        "failed_phase": phase,
        "recommended_action": _QUARANTINE_ACTIONS.get(classification, "Investigar manualmente"),
    }
    out_path = Path(quarantine_dir) / f"{Path(file_name).stem}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(record, f, indent=2, ensure_ascii=False)
    return record
