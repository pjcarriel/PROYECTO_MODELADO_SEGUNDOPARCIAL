import os
import re
import json
import yaml
import uuid
import hashlib
from pathlib import Path
from datetime import datetime, timezone

def generate_process_id() -> str:
    """Genera un UUID v4 único por ejecución."""
    return str(uuid.uuid4())

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def load_config(config_path: str = "config/etl_config.yaml") -> dict:
    """Lee el archivo YAML de configuración y valida secciones mínimas."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"No existe el archivo de configuración: {config_path}")
    with path.open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    validate_config(config)
    return config

def validate_config(config: dict) -> None:
    required_sections = ["project", "paths", "spark", "flags", "database", "downloads", "files"]
    missing = [s for s in required_sections if s not in config]
    if missing:
        raise ValueError(f"Faltan secciones obligatorias en etl_config.yaml: {missing}")
    required_paths = ["raw", "bronze", "silver", "gold", "quarantine", "audit", "metadata", "logs"]
    missing_paths = [p for p in required_paths if p not in config["paths"]]
    if missing_paths:
        raise ValueError(f"Faltan rutas obligatorias en config.paths: {missing_paths}")
    required_spark = ["master", "app_name", "executor_memory", "driver_memory", "shuffle_partitions"]
    missing_spark = [s for s in required_spark if s not in config["spark"]]
    if missing_spark:
        raise ValueError(f"Faltan parámetros Spark obligatorios: {missing_spark}")

def resolve_base_dir(config: dict) -> Path:
    base_dir = config.get("project", {}).get("base_dir", ".")
    return Path(base_dir).resolve()

def resolve_path(config: dict, key: str) -> Path:
    return resolve_base_dir(config) / config["paths"][key]

def ensure_directories(config: dict) -> None:
    """Crea las carpetas principales del proyecto si no existen."""
    for key in ["raw", "bronze", "silver", "gold", "quarantine", "audit", "metadata", "logs"]:
        resolve_path(config, key).mkdir(parents=True, exist_ok=True)

def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def sha256_file(file_path: str, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()

def parse_year_month(file_name: str):
    """
    Extrae año y mes desde nombres como yellow_tripdata_2023-01.parquet.
    Retorna (None, None) si no se encuentra.
    """
    m = re.search(r"(\d{4})-(\d{2})", file_name)
    if not m:
        return None, None
    return int(m.group(1)), int(m.group(2))

def infer_service_type_from_path(file_path: str) -> str:
    lower = str(file_path).lower()
    name = Path(file_path).name.lower()
    if "bad_parquet" in lower:
        return "bad_parquet"
    if name.startswith("yellow_") or "/yellow/" in lower or "\\yellow\\" in lower:
        return "yellow"
    if name.startswith("green_") or "/green/" in lower or "\\green\\" in lower:
        return "green"
    if name.startswith("fhvhv_") or "/fhvhv/" in lower or "\\fhvhv\\" in lower:
        return "fhvhv"
    return "unknown"

def infer_source_system(service_type: str) -> str:
    return "APACHE_PARQUET_TESTING" if service_type == "bad_parquet" else "NYC_TLC"

def get_spark_session(config: dict):
    import os
    from pyspark.sql import SparkSession

    spark_cfg = config["spark"]

    # --add-opens con formato =<módulo>=ALL-UNNAMED requerido para Java 17/21.
    # Se setea JAVA_TOOL_OPTIONS ANTES de crear la sesión para que la JVM
    # los tome desde el inicio (spark.driver.extraJavaOptions llega tarde en notebooks).
    java_opens = (
        "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
        "--add-opens=java.base/java.io=ALL-UNNAMED "
        "--add-opens=java.base/java.net=ALL-UNNAMED "
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/java.util=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
        "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED "
        "-Dio.netty.noUnsafe=true"
    )
    # Sobreescribir solo si no está ya configurado para no interferir con otros procesos
    if "JAVA_TOOL_OPTIONS" not in os.environ:
        os.environ["JAVA_TOOL_OPTIONS"] = java_opens

    spark = (
        SparkSession.builder
        .master(str(spark_cfg["master"]))
        .appName(str(spark_cfg["app_name"]))
        .config("spark.executor.memory", str(spark_cfg["executor_memory"]))
        .config("spark.driver.memory", str(spark_cfg["driver_memory"]))
        .config("spark.sql.shuffle.partitions", str(spark_cfg["shuffle_partitions"]))
        .config("spark.sql.parquet.mergeSchema", "false")
        .config("spark.sql.files.ignoreCorruptFiles", "false")
        .config("spark.driver.extraJavaOptions", java_opens)
        .config("spark.executor.extraJavaOptions", java_opens)
        # Arrow DESHABILITADO: la combinación Arrow+Netty bundled en PySpark 4.x
        # produce crash del hilo JVM "serve-Arrow" (UnsupportedOperationException en
        # PooledByteBufAllocatorL). Python queda colgado esperando al hilo muerto.
        # La serialización row-by-row (sin Arrow) es lenta para DFs grandes, por eso
        # las conversiones pandas↔Spark de millones de filas se hacen vía pyarrow directamente.
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")
        # Hadoop security=simple evita Subject.getSubject() (removido en Java 21) y permite
        # que spark.read.parquet() / spark.write.parquet() lean/escriban el filesystem local.
        .config("spark.hadoop.hadoop.security.authentication", "simple")
        .getOrCreate()
    )
    return spark

def read_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def list_parquet_files(folder: str):
    folder_path = Path(folder)
    if not folder_path.exists():
        return []
    return sorted([str(p) for p in folder_path.rglob("*.parquet")])
