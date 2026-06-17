# Proyecto II Parcial — Modelado Avanzado de Base de Datos
## Pipeline ETL Avanzado con Apache Spark / PySpark — NYC Taxi & Limousine Commission

Pipeline ETL completo que procesa datos reales de viajes de taxi de Nueva York usando la arquitectura medallón (Raw → Bronze → Silver → Gold) con carga final a Supabase (PostgreSQL).

---

## Arquitectura del pipeline

```
data/raw/          →   data/bronze/      →   data/silver/      →   data/gold/
Parquet originales      Esquema canónico       Datos validados        Tablas Supabase
(NYC TLC)               24 campos              13 reglas calidad      + reporte JSON
```

```
data/quarantine/   — Archivos corruptos o ilegibles
data/audit/        — quality_rejected_records + quality_metrics_summary + audit_file_inventory
```

---

## Estructura del proyecto

```
Proyecto_Grupo1/
├── config/
│   └── etl_config.yaml                  # Configuración central del pipeline
├── data/
│   ├── raw/                             # Parquet originales descargados
│   ├── bronze/                          # Esquema canónico (24 campos)
│   ├── silver/                          # Datos validados y enriquecidos
│   ├── gold/                            # quality_report.json
│   ├── quarantine/                      # Archivos corruptos
│   └── audit/                           # Tablas de auditoría y calidad
├── metadata/
│   ├── expected_schema_yellow.json
│   ├── expected_schema_green.json
│   ├── expected_schema_fhvhv.json
│   ├── canonical_schema_trips.json      # 24 campos del esquema canónico
│   └── business_rules.json              # Reglas de homologación y calidad
├── notebooks/
│   ├── 01_extraccion.ipynb
│   ├── 02_diagnostico_reconstruccion.ipynb
│   ├── 03_transformacion_validacion.ipynb
│   ├── 04_carga_base_datos.ipynb
│   └── 05_reporte_calidad_conclusiones.ipynb
├── src/
│   ├── extract.py                       # Descarga y auditoría de archivos
│   ├── schema_recovery.py               # Homologación al esquema canónico
│   ├── quality_rules.py                 # 13 reglas de validación
│   ├── load.py                          # Carga a Supabase por lotes (upsert)
│   └── utils.py                         # Spark session, config, rutas
├── supabase_schema.sql                  # DDL completo para crear tablas en Supabase
└── README.md
```

---

## Requisitos

- Python 3.10+
- Java 17 (requerido por PySpark — descargar desde [Adoptium](https://adoptium.net))
- Jupyter Lab o Jupyter Notebook

Instalar dependencias:

```bash
pip install pyspark pyyaml supabase pyarrow pandas matplotlib seaborn psycopg2-binary
```

Configurar variable de entorno de Java (Windows):

```
JAVA_HOME = C:\Program Files\Eclipse Adoptium\jdk-17.x.x-hotspot
```

---

## Configuración de Supabase

1. Crear un proyecto en [supabase.com](https://supabase.com)
2. Ir a **Settings → API Keys** y copiar la **Secret key** (`service_role`)
3. Ejecutar `supabase_schema.sql` en el **SQL Editor** de Supabase para crear las 7 tablas
4. Actualizar las credenciales en los notebooks 04 y 05:

```python
SUPABASE_URL = "https://tu-proyecto.supabase.co"
SUPABASE_KEY = "eyJ..."  # service_role key
```

---

## Orden de ejecución

Ejecutar los notebooks en este orden desde Jupyter:

### NB01 — Extracción y auditoría
`notebooks/01_extraccion.ipynb`

- Descarga 12 archivos Parquet reales de NYC TLC (yellow, green, fhvhv) desde 2020 a 2023
- Descarga archivos problemáticos desde `apache/parquet-testing/bad_data`
- Genera `data/audit/audit_file_inventory/` con inventario técnico de cada archivo

Archivos descargados:

| Servicio | Período |
|----------|---------|
| yellow   | 2020-01, 2021-01, 2022-01, 2022-02, 2023-01, 2023-02, 2023-03, 2023-04 |
| green    | 2023-01, 2023-02 |
| fhvhv    | 2023-01 |

---

### NB02 — Diagnóstico y reconstrucción Bronze
`notebooks/02_diagnostico_reconstruccion.ipynb`

- Lee cada archivo Parquet con PyArrow (sin conversión Spark, para máxima velocidad)
- Homologa los 3 esquemas distintos (yellow/green/fhvhv) a 24 campos canónicos
- Genera `trip_id` único por viaje mediante hash SHA-256
- Escribe `data/bronze/` particionado por `service_type / year / month`
- Detecta y mueve archivos corruptos a `data/quarantine/`

Resultado: **44.5 millones de registros** en bronze.

---

### NB03 — Transformaciones y validación Silver
`notebooks/03_transformacion_validacion.ipynb`

- Lee bronze con Spark y aplica 13 reglas de calidad
- Calcula campos derivados: `trip_duration_minutes`, `average_speed_mph`, `fare_per_mile`, `tip_percentage`, `is_airport_trip`, `is_suspicious_trip`
- Escribe `data/silver/` con registros válidos
- Escribe `data/audit/quality_rejected_records/` y `data/audit/quality_metrics_summary/`

Reglas de calidad aplicadas:

| Regla | Descripción |
|-------|-------------|
| NULL_CRITICAL | Campos obligatorios nulos |
| INVALID_DATE_RANGE | Fechas fuera del rango 2019-2023 |
| NEGATIVE_FARE | Tarifa negativa |
| ZERO_DISTANCE | Distancia <= 0 |
| DUPLICATE_TRIP_ID | trip_id duplicado |
| INVALID_PASSENGER | Pasajeros fuera de rango |
| FUTURE_DATE | Fecha de pickup en el futuro |
| DROPOFF_BEFORE_PICKUP | Dropoff anterior al pickup |
| EXCESSIVE_DURATION | Duración > 24 horas |
| EXCESSIVE_DISTANCE | Distancia > 500 millas |
| INVALID_LOCATION | Location ID fuera de rango |
| SUSPICIOUS_FARE | Tarifa anómalamente alta |
| SUSPICIOUS_SPEED | Velocidad promedio > 100 mph |

Resultado: **43.9 millones de registros válidos** en silver (99.07% calidad).

---

### NB04 — Carga a Supabase
`notebooks/04_carga_base_datos.ipynb`

- Lee silver con pandas (por partición, eficiente en memoria)
- Carga 6 tablas en Supabase mediante upsert idempotente por lotes de 500 registros

| Tabla | Descripción |
|-------|-------------|
| `gold_trips_clean` | Muestra representativa de 100,000 viajes válidos |
| `gold_daily_revenue` | Resumen diario de ingresos por servicio |
| `gold_location_performance` | Rendimiento por zona origen-destino |
| `quality_metrics` | Métricas de calidad por servicio/año/mes |
| `quality_rejected_records` | Registros rechazados con motivo |
| `audit_file_inventory` | Inventario técnico de archivos procesados |

La tabla `load_audit` registra automáticamente cada operación de carga para trazabilidad.

---

### NB05 — Reporte de calidad y conclusiones
`notebooks/05_reporte_calidad_conclusiones.ipynb`

- Genera visualizaciones del pipeline completo
- Ejecuta las 3 consultas SQL obligatorias del enunciado
- Exporta `data/gold/quality_report.json` con todos los KPIs

Visualizaciones generadas:

1. KPIs globales de calidad (total, válidos, rechazados, sospechosos)
2. Rechazos por regla de calidad (barras horizontales)
3. Distribución de viajes válidos por tipo de servicio
4. Evolución del porcentaje de calidad por mes (2019-2023)
5. Distribuciones de `trip_distance` y `fare_amount` + scatter tarifa vs distancia
6. Proporción de viajes sospechosos (donut + barras)
7. Validación de integridad (duplicados y distancias inválidas en silver)

Consultas SQL obligatorias ejecutadas:

```sql
-- 1. Viajes y revenue por tipo de servicio
SELECT service_type, COUNT(*) AS total_trips, SUM(total_amount) AS total_revenue
FROM gold_trips_clean GROUP BY service_type ORDER BY total_revenue DESC;

-- 2. Métricas de calidad por servicio, año y mes
SELECT service_type, year, month, total_records, valid_records,
       rejected_records, quality_percentage
FROM quality_metrics ORDER BY year, month, service_type;

-- 3. Top 20 rutas por revenue
SELECT pickup_location_id, dropoff_location_id,
       COUNT(*) AS total_trips, SUM(total_amount) AS total_revenue,
       AVG(trip_duration_minutes) AS avg_duration
FROM gold_trips_clean
GROUP BY pickup_location_id, dropoff_location_id
ORDER BY total_revenue DESC LIMIT 20;
```

---

## Tablas en Supabase

El archivo `supabase_schema.sql` contiene el DDL completo de las 7 tablas. Ejecutarlo en el SQL Editor antes de correr NB04.

```
gold_trips_clean          — 31 columnas, PK: trip_id
gold_daily_revenue        — PK: (service_type, trip_date)
gold_location_performance — PK: (service_type, pickup_location_id, dropoff_location_id)
quality_metrics           — PK: (process_id, service_type, year, month)
quality_rejected_records  — PK: (process_id, trip_id, rejection_rule)
audit_file_inventory      — PK: (process_id, file_name)
load_audit                — auditoría de cada operación de carga
```

---

## Resultados finales

| Métrica | Valor |
|---------|-------|
| Archivos procesados | 12 Parquet NYC TLC |
| Registros en bronze | 44,502,927 |
| Registros en silver (válidos) | 43,928,085 |
| Registros rechazados | 302,148 (0.93%) |
| Porcentaje de calidad global | 99.07% |
| Tablas cargadas en Supabase | 7 |
| Duplicados en gold_trips_clean | 0 |
