"""
Persona 3 — Carga a Supabase (PostgreSQL) con supabase-py.
Tablas obligatorias según el PDF del proyecto:
  gold_trips_clean, gold_daily_revenue, gold_location_performance,
  quality_rejected_records, quality_metrics_summary, audit_file_inventory
"""
import math
import pandas as pd

from supabase import create_client, Client

from utils import resolve_path


def get_supabase_client(supabase_url: str, supabase_key: str) -> Client:
    """Crea y retorna un cliente Supabase autenticado."""
    return create_client(supabase_url, supabase_key)


def _to_pd(df):
    """Acepta Spark DataFrame o pandas DataFrame — devuelve siempre pandas."""
    return df.toPandas() if hasattr(df, "toPandas") else df


def _df_to_records(df_pd) -> list:
    """
    Convierte un DataFrame pandas a lista de dicts listos para Supabase.
    Normaliza: NaN → None, datetime/Timestamp → ISO string, bool → bool nativo.
    """
    records = []
    for row in df_pd.to_dict("records"):
        clean = {}
        for k, v in row.items():
            if isinstance(v, float) and math.isnan(v):
                clean[k] = None
            elif hasattr(v, "isoformat"):
                clean[k] = v.isoformat()
            elif hasattr(v, "item"):
                clean[k] = v.item()
            else:
                clean[k] = v
        records.append(clean)
    return records


def _upsert_batched(supabase: Client, table: str, records: list,
                    on_conflict: str, batch_size: int = 500,
                    process_id: str = None) -> dict:
    """Helper interno: upsert en lotes con auditoría opcional."""
    total = len(records)
    inserted = 0
    errors = []

    for i in range(0, total, batch_size):
        batch = records[i: i + batch_size]
        try:
            supabase.table(table).upsert(batch, on_conflict=on_conflict).execute()
            inserted += len(batch)
            print(f"  [{table}] Lote {i // batch_size + 1}: {inserted}/{total}")
        except Exception as exc:
            errors.append(str(exc)[:500])
            print(f"  [{table}] ERROR lote {i // batch_size + 1}: {exc}")

    status = "SUCCESS" if not errors else "PARTIAL"

    if process_id:
        supabase.table("load_audit").insert({
            "process_id": process_id,
            "table_name": table,
            "rows_inserted": inserted,
            "status": status,
            "error_message": ("; ".join(errors))[:2000] if errors else None,
        }).execute()

    return {"rows_inserted": inserted, "total": total, "status": status, "errors": errors}


# ─────────────────────────────────────────────────────────────────────────────
# Tabla 1: gold_trips_clean
# ─────────────────────────────────────────────────────────────────────────────
def load_trips_to_supabase(
    df_silver,
    supabase: Client,
    process_id: str,
    batch_size: int = 500,
) -> dict:
    """
    Carga el DataFrame silver a la tabla gold_trips_clean de Supabase en lotes.
    Upsert con trip_id como clave de conflicto → idempotente.
    """
    df_pd = _to_pd(df_silver)

    for bool_col in ["is_airport_trip", "is_suspicious_trip"]:
        if bool_col in df_pd.columns:
            df_pd[bool_col] = df_pd[bool_col].astype(bool)

    records = _df_to_records(df_pd)
    return _upsert_batched(supabase, "gold_trips_clean", records,
                           on_conflict="trip_id", batch_size=batch_size,
                           process_id=process_id)


# ─────────────────────────────────────────────────────────────────────────────
# Tabla 2: gold_daily_revenue
# ─────────────────────────────────────────────────────────────────────────────
def load_gold_daily_revenue_to_supabase(
    df_daily,
    supabase: Client,
    process_id: str,
    batch_size: int = 500,
) -> dict:
    """Carga el resumen diario de ingresos a la tabla gold_daily_revenue."""
    records = _df_to_records(_to_pd(df_daily))
    return _upsert_batched(supabase, "gold_daily_revenue", records,
                           on_conflict="service_type,trip_date",
                           batch_size=batch_size, process_id=process_id)


# ─────────────────────────────────────────────────────────────────────────────
# Tabla 3: gold_location_performance
# ─────────────────────────────────────────────────────────────────────────────
def load_gold_location_performance_to_supabase(
    df_location,
    supabase: Client,
    process_id: str,
    batch_size: int = 500,
) -> dict:
    """Carga el rendimiento por zona a la tabla gold_location_performance."""
    records = _df_to_records(_to_pd(df_location))
    return _upsert_batched(supabase, "gold_location_performance", records,
                           on_conflict="service_type,pickup_location_id,dropoff_location_id",
                           batch_size=batch_size, process_id=process_id)


# ─────────────────────────────────────────────────────────────────────────────
# Tabla 4: quality_rejected_records
# ─────────────────────────────────────────────────────────────────────────────
def load_rejected_records_to_supabase(
    config: dict,
    supabase: Client,
    process_id: str,
    spark,
    batch_size: int = 500,
) -> dict:
    """Lee quality_rejected_records desde data/audit/ y lo carga en Supabase."""
    audit_path = resolve_path(config, "audit")
    rejected_path = str(audit_path / "quality_rejected_records")
    records = _df_to_records(pd.read_parquet(rejected_path))
    print(f"quality_rejected_records: {len(records)} filas a cargar")
    return _upsert_batched(supabase, "quality_rejected_records", records,
                           on_conflict="process_id,trip_id,rejection_rule",
                           batch_size=batch_size, process_id=process_id)


# ─────────────────────────────────────────────────────────────────────────────
# Tabla 5: quality_metrics_summary
# ─────────────────────────────────────────────────────────────────────────────
def load_quality_metrics_to_supabase(
    config: dict,
    supabase: Client,
    process_id: str,
    spark,
) -> dict:
    """Lee quality_metrics_summary desde data/audit/ y lo carga en Supabase."""
    audit_path = resolve_path(config, "audit")
    metrics_path = str(audit_path / "quality_metrics_summary")
    records = _df_to_records(pd.read_parquet(metrics_path))

    supabase.table("quality_metrics").upsert(records).execute()
    supabase.table("load_audit").insert({
        "process_id": process_id,
        "table_name": "quality_metrics",
        "rows_inserted": len(records),
        "status": "SUCCESS",
        "error_message": None,
    }).execute()

    print(f"quality_metrics cargado: {len(records)} filas")
    return {"rows_inserted": len(records), "status": "SUCCESS"}


# ─────────────────────────────────────────────────────────────────────────────
# Tabla 6: audit_file_inventory
# ─────────────────────────────────────────────────────────────────────────────
def load_audit_inventory_to_supabase(
    config: dict,
    supabase: Client,
    process_id: str,
    spark,
    batch_size: int = 200,
) -> dict:
    """Lee audit_file_inventory desde data/audit/ y lo carga en Supabase."""
    audit_path = resolve_path(config, "audit")
    inventory_path = str(audit_path / "audit_file_inventory")
    records = _df_to_records(pd.read_parquet(inventory_path))
    print(f"audit_file_inventory: {len(records)} filas a cargar")
    return _upsert_batched(supabase, "audit_file_inventory", records,
                           on_conflict="process_id,file_name",
                           batch_size=batch_size, process_id=process_id)
