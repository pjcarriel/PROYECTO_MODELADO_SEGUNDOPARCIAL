# Proyecto II Parcial — Persona 1  
## Pipeline ETL Avanzado con Apache Spark / PySpark

Este paquete corresponde a la **Persona 1: Extracción, Infraestructura y Auditoría**.  
Cubre las fases iniciales del proyecto: creación de estructura, descarga de archivos Parquet, lectura segura con Spark, inventario técnico y diagnóstico de esquemas.

## 1. Responsabilidad de Persona 1

La Persona 1 debe entregar:

- `notebooks/01_extraccion.ipynb`
- `notebooks/02_diagnostico_reconstruccion.ipynb` con la sección de diagnóstico
- `config/etl_config.yaml`
- `src/extract.py`
- `src/utils.py`
- `src/schema_recovery.py` para diagnóstico
- `metadata/expected_schema_yellow.json`
- `metadata/expected_schema_green.json`
- `metadata/expected_schema_fhvhv.json`
- `metadata/canonical_schema_trips.json`
- `metadata/business_rules.json`
- `data/audit/audit_file_inventory/`
- `README.md`

## 2. Preparación del entorno

En Google Colab o Jupyter, ejecutar:

```bash
pip install pyspark pyyaml requests
```

Después, abrir los notebooks en este orden:

1. `notebooks/01_extraccion.ipynb`
2. `notebooks/02_diagnostico_reconstruccion.ipynb`

## 3. Descarga de datos

Los archivos configurados están en:

```text
config/etl_config.yaml
```

El notebook 01 descarga:

- Archivos reales de NYC TLC:
  - yellow 2023-01, 2023-02, 2023-03, 2023-04
  - green 2023-01, 2023-02
  - fhvhv 2023-01
  - yellow 2022-01, 2022-02, 2021-01, 2020-01
- Archivos problemáticos desde `apache/parquet-testing/bad_data`.

## 4. Resultado principal

La tabla de auditoría se genera en:

```text
data/audit/audit_file_inventory/
```

Campos generados:

```text
process_id
source_system
service_type
file_name
file_path
file_size_mb
partition_year
partition_month
read_status
record_count
column_count
schema_hash
error_message
processed_at
```

## 5. Idempotencia

La escritura de `audit_file_inventory` usa modo `overwrite`, por lo tanto, si se ejecuta dos veces no duplica los registros finales.

## 6. Diagnóstico de esquemas

El notebook 02 compara los archivos con lectura exitosa contra:

```text
metadata/expected_schema_yellow.json
metadata/expected_schema_green.json
metadata/expected_schema_fhvhv.json
```

Genera la tabla:

```text
data/audit/schema_differences/
```

con columnas extra, columnas faltantes y tipos incompatibles.

## 7. Nota para integración con el equipo

La Persona 2 debe usar:

- `data/audit/audit_file_inventory/`
- `metadata/canonical_schema_trips.json`
- `metadata/business_rules.json`

para continuar con la reconstrucción canónica y las transformaciones.
