import json
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
    """
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
        df = spark.read.parquet(r["file_path"])
        actual = spark_schema_map(df)
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
    output_path = resolve_path(config, "audit") / "schema_differences"
    df.write.mode("overwrite").parquet(str(output_path))
    return str(output_path)
