import os
import json
import shutil
from pathlib import Path
from urllib.request import urlretrieve, Request, urlopen
from urllib.error import URLError, HTTPError
from datetime import datetime, timezone

from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, LongType, TimestampType
)
from pyspark.sql.functions import current_timestamp

from utils import (
    resolve_base_dir, resolve_path, ensure_directories, sha256_text, sha256_file,
    parse_year_month, infer_service_type_from_path, infer_source_system, utc_now_iso
)

AUDIT_COLUMNS = [
    "process_id",
    "source_system",
    "service_type",
    "file_name",
    "file_path",
    "file_size_mb",
    "file_hash_sha256",
    "partition_year",
    "partition_month",
    "read_status",
    "record_count",
    "column_count",
    "schema_hash",
    "error_message",
    "processed_at",
]

AUDIT_SCHEMA = StructType([
    StructField("process_id", StringType(), False),
    StructField("source_system", StringType(), True),
    StructField("service_type", StringType(), True),
    StructField("file_name", StringType(), True),
    StructField("file_path", StringType(), True),
    StructField("file_size_mb", DoubleType(), True),
    StructField("file_hash_sha256", StringType(), True),
    StructField("partition_year", IntegerType(), True),
    StructField("partition_month", IntegerType(), True),
    StructField("read_status", StringType(), True),
    StructField("record_count", LongType(), True),
    StructField("column_count", IntegerType(), True),
    StructField("schema_hash", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("processed_at", TimestampType(), True),
])

def _download_url(url: str, target_path: str, overwrite: bool = False) -> str:
    target = Path(target_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() and target.stat().st_size > 0 and not overwrite:
        return "SKIPPED_EXISTS"
    try:
        urlretrieve(url, str(target))
        return "DOWNLOADED"
    except Exception as e:
        return f"ERROR: {e}"

def download_nyc_files(config: dict, overwrite: bool = False) -> list:
    """
    Descarga los archivos NYC TLC definidos en config/files y conserva la estructura raw particionada.
    """
    results = []
    base_dir = resolve_base_dir(config)
    for item in config["files"]:
        url = item["url"]
        raw_relative_path = item["raw_relative_path"]
        target_path = base_dir / raw_relative_path
        status = _download_url(url, str(target_path), overwrite=overwrite)
        file_hash = sha256_file(str(target_path)) if target_path.exists() and target_path.stat().st_size > 0 else None
        results.append({
            "file_name": item["file_name"],
            "service_type": item["service_type"],
            "year": int(item["year"]),
            "month": int(item["month"]),
            "url": url,
            "target_path": str(target_path),
            "download_status": status,
            "sha256_file": file_hash
        })
    return results

def _github_api_get_json(url: str):
    req = Request(url, headers={"User-Agent": "etl-spark-parquet-advanced"})
    with urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))

def download_apache_bad_data(config: dict, overwrite: bool = False) -> list:
    """
    Descarga archivos problemáticos desde apache/parquet-testing/bad_data usando GitHub API.
    No se detiene si un archivo falla.
    """
    api_url = config["downloads"]["apache_parquet_testing_bad_data_api"]
    max_files = int(config["downloads"].get("apache_parquet_testing_max_files", 25))
    target_dir = resolve_path(config, "raw") / "bad_parquet"
    target_dir.mkdir(parents=True, exist_ok=True)

    results = []
    try:
        entries = _github_api_get_json(api_url)
    except Exception as e:
        return [{"file_name": None, "download_status": f"ERROR_LISTING_BAD_DATA: {e}", "target_path": str(target_dir)}]

    count = 0
    for entry in entries:
        if count >= max_files:
            break
        if entry.get("type") != "file":
            continue
        download_url = entry.get("download_url")
        name = entry.get("name")
        if not download_url:
            continue
        target_path = target_dir / name
        status = _download_url(download_url, str(target_path), overwrite=overwrite)
        file_hash = sha256_file(str(target_path)) if target_path.exists() and target_path.stat().st_size > 0 else None
        results.append({
            "file_name": name,
            "url": download_url,
            "target_path": str(target_path),
            "download_status": status,
            "sha256_file": file_hash
        })
        count += 1
    return results

def collect_configured_file_paths(config: dict, include_bad_parquet: bool = True) -> list:
    """
    Retorna las rutas esperadas para los archivos NYC TLC y los archivos bad_parquet existentes.
    """
    base_dir = resolve_base_dir(config)
    records = []
    for item in config["files"]:
        raw_path = base_dir / item["raw_relative_path"]
        records.append({
            "source_system": item.get("source_system", "NYC_TLC"),
            "service_type": item["service_type"],
            "file_path": str(raw_path),
            "partition_year": int(item["year"]),
            "partition_month": int(item["month"]),
        })

    if include_bad_parquet:
        bad_dir = resolve_path(config, "raw") / "bad_parquet"
        for p in sorted(bad_dir.rglob("*")):
            if p.is_file() and p.name != ".gitkeep":
                records.append({
                    "source_system": "APACHE_PARQUET_TESTING",
                    "service_type": "bad_parquet",
                    "file_path": str(p.resolve()),
                    "partition_year": None,
                    "partition_month": None,
                })
    return records

def read_file_safely(spark, file_meta: dict, process_id: str) -> dict:
    """
    Lee un archivo Parquet de forma individual y devuelve un registro para audit_file_inventory.
    Si falla, captura la excepción sin detener el pipeline.
    """
    file_path = Path(file_meta["file_path"]).resolve()
    file_name = file_path.name
    service_type = file_meta.get("service_type") or infer_service_type_from_path(str(file_path))
    source_system = file_meta.get("source_system") or infer_source_system(service_type)
    year = file_meta.get("partition_year")
    month = file_meta.get("partition_month")
    if year is None or month is None:
        parsed_year, parsed_month = parse_year_month(file_name)
        year = year if year is not None else parsed_year
        month = month if month is not None else parsed_month

    processed_at = datetime.now(timezone.utc).replace(tzinfo=None)
    file_size_mb = round(file_path.stat().st_size / 1048576, 2) if file_path.exists() else 0.0

    file_hash = sha256_file(str(file_path)) if file_path.exists() and file_path.stat().st_size > 0 else None

    record = {
        "process_id": process_id,
        "source_system": source_system,
        "service_type": service_type,
        "file_name": file_name,
        "file_path": str(file_path),
        "file_size_mb": file_size_mb,
        "file_hash_sha256": file_hash,
        "partition_year": int(year) if year is not None else None,
        "partition_month": int(month) if month is not None else None,
        "read_status": None,
        "record_count": None,
        "column_count": None,
        "schema_hash": None,
        "error_message": None,
        "processed_at": processed_at,
    }

    if not file_path.exists():
        record["read_status"] = "CORRUPT"
        record["error_message"] = "FILE_NOT_FOUND"
        return record

    if file_path.stat().st_size == 0:
        record["read_status"] = "EMPTY"
        record["record_count"] = 0
        record["column_count"] = 0
        record["schema_hash"] = sha256_text("EMPTY_FILE")
        return record

    try:
        import pyarrow.parquet as pq
        # Abrimos con open() nativo de Python para evitar que pyarrow use el
        # filesystem Hadoop registrado por la JVM de PySpark, lo cual provoca
        # "getSubject is not supported" en Java 17/21.
        with open(str(file_path), "rb") as f:
            pf     = pq.ParquetFile(f)
            count  = pf.metadata.num_rows
            schema = pf.schema_arrow
        record["read_status"]   = "SUCCESS" if count > 0 else "EMPTY"
        record["record_count"]  = int(count)
        record["column_count"]  = len(schema)
        record["schema_hash"]   = sha256_text(str(schema))
        record["error_message"] = None
    except Exception as e:
        error_text = str(e)
        if "schema" in error_text.lower() or "column" in error_text.lower():
            record["read_status"] = "SCHEMA_ERROR"
        elif file_path.stat().st_size == 0:
            record["read_status"] = "EMPTY"
        else:
            record["read_status"] = "CORRUPT"
        record["record_count"]  = None
        record["column_count"]  = None
        record["schema_hash"]   = None
        record["error_message"] = error_text[:4000]
    return record

def build_audit_file_inventory(spark, config: dict, process_id: str):
    """
    Construye audit_file_inventory con exactamente los 15 campos requeridos.
    """
    ensure_directories(config)
    file_metas = collect_configured_file_paths(config, include_bad_parquet=True)
    rows = [read_file_safely(spark, meta, process_id) for meta in file_metas]
    return spark.createDataFrame(rows, schema=AUDIT_SCHEMA).select(*AUDIT_COLUMNS)

def write_audit_inventory(df, config: dict):
    """
    Escribe la tabla en data/audit/audit_file_inventory en modo overwrite para garantizar idempotencia.
    Usa pandas+pyarrow para escribir el parquet, evitando el error getSubject de Java 17/18+.
    """
    import shutil
    audit_path = resolve_path(config, "audit") / "audit_file_inventory"
    overwrite = config.get("flags", {}).get("overwrite_audit_inventory", True)
    if overwrite and audit_path.exists():
        shutil.rmtree(str(audit_path))
    audit_path.mkdir(parents=True, exist_ok=True)
    out_file = audit_path / "part-00000.parquet"
    df.toPandas().to_parquet(str(out_file), index=False, engine="pyarrow")
    return str(audit_path)

def read_partitioned_service_folder(spark, config: dict, service_type: str):
    """
    Lee una carpeta particionada completa de la capa raw por servicio.
    """
    service_path = resolve_path(config, "raw") / service_type
    return spark.read.option("basePath", str(service_path)).parquet(str(service_path))
