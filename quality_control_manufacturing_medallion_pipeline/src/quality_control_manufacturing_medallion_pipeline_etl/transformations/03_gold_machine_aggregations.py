"""
03_gold_machine_aggregations.py
================================
Capa Oro — Agregaciones dinámicas de comportamiento por máquina.

Calcula métricas históricas en ventanas temporales múltiples:
  - 1 hora   : alertas en tiempo real
  - 24 horas : tendencia diaria
  - 7 días   : tendencia semanal
  - 30 días  : baseline mensual

Tabla generada:
  - gold_machine_aggregations : métricas por machine_id y ventana temporal
"""

import pyspark.pipelines as dp
from pyspark.sql import functions as F

CATALOG       = "workspace"
SCHEMA_TABLES = "ana_martin17"
WATERMARK     = "30 days"


# =============================================================================
# VISTAS TEMPORALES — una por ventana temporal
# =============================================================================

@dp.view
def agg_1h():
    return (
        spark.readStream.table(f"{CATALOG}.{SCHEMA_TABLES}.silver_inspections_labeled")
        .withWatermark("timestamp", WATERMARK)
        .groupBy(
            "machine_id", "line_id",
            F.window("timestamp", "1 hour").alias("window")
        )
        .agg(
            F.count("unit_id").alias("total_units_1h"),
            F.sum("is_defective").alias("defects_1h"),
            F.mean("vibration_mm_s").alias("avg_vibration_1h"),
            F.mean("tool_wear_pct").alias("avg_tool_wear_1h"),
            F.mean("temperature_celsius").alias("avg_temperature_1h"),
            F.mean("solder_thickness_um").alias("avg_solder_thickness_1h"),
            F.mean("alignment_error_um").alias("avg_alignment_error_1h"),
        )
        .select(
            "machine_id", "line_id",
            F.col("window.end").alias("window_end"),
            "total_units_1h", "defects_1h",
            (F.col("defects_1h") / F.col("total_units_1h")).alias("defect_rate_1h"),
            "avg_vibration_1h", "avg_tool_wear_1h", "avg_temperature_1h",
            "avg_solder_thickness_1h", "avg_alignment_error_1h",
        )
    )


@dp.view
def agg_24h():
    return (
        spark.readStream.table(f"{CATALOG}.{SCHEMA_TABLES}.silver_inspections_labeled")
        .withWatermark("timestamp", WATERMARK)
        .groupBy(
            "machine_id", "line_id",
            F.window("timestamp", "24 hours").alias("window")
        )
        .agg(
            F.count("unit_id").alias("total_units_24h"),
            F.sum("is_defective").alias("defects_24h"),
            F.mean("vibration_mm_s").alias("avg_vibration_24h"),
            F.mean("tool_wear_pct").alias("avg_tool_wear_24h"),
            F.mean("temperature_celsius").alias("avg_temperature_24h"),
            F.mean("solder_thickness_um").alias("avg_solder_thickness_24h"),
            F.mean("alignment_error_um").alias("avg_alignment_error_24h"),
        )
        .select(
            "machine_id", "line_id",
            F.col("window.end").alias("window_end"),
            "total_units_24h", "defects_24h",
            (F.col("defects_24h") / F.col("total_units_24h")).alias("defect_rate_24h"),
            "avg_vibration_24h", "avg_tool_wear_24h", "avg_temperature_24h",
            "avg_solder_thickness_24h", "avg_alignment_error_24h",
        )
    )


@dp.view
def agg_7d():
    return (
        spark.readStream.table(f"{CATALOG}.{SCHEMA_TABLES}.silver_inspections_labeled")
        .withWatermark("timestamp", WATERMARK)
        .groupBy(
            "machine_id", "line_id",
            F.window("timestamp", "7 days").alias("window")
        )
        .agg(
            F.count("unit_id").alias("total_units_7d"),
            F.sum("is_defective").alias("defects_7d"),
            F.mean("vibration_mm_s").alias("avg_vibration_7d"),
            F.mean("tool_wear_pct").alias("avg_tool_wear_7d"),
            F.mean("solder_thickness_um").alias("avg_solder_thickness_7d"),
        )
        .select(
            "machine_id", "line_id",
            F.col("window.end").alias("window_end"),
            "total_units_7d", "defects_7d",
            (F.col("defects_7d") / F.col("total_units_7d")).alias("defect_rate_7d"),
            "avg_vibration_7d", "avg_tool_wear_7d", "avg_solder_thickness_7d",
        )
    )


@dp.view
def agg_30d():
    return (
        spark.readStream.table(f"{CATALOG}.{SCHEMA_TABLES}.silver_inspections_labeled")
        .withWatermark("timestamp", WATERMARK)
        .groupBy(
            "machine_id", "line_id",
            F.window("timestamp", "30 days").alias("window")
        )
        .agg(
            F.count("unit_id").alias("total_units_30d"),
            F.sum("is_defective").alias("defects_30d"),
            F.mean("vibration_mm_s").alias("avg_vibration_30d"),
            F.mean("tool_wear_pct").alias("avg_tool_wear_30d"),
            F.mean("solder_thickness_um").alias("avg_solder_thickness_30d"),
        )
        .select(
            "machine_id", "line_id",
            F.col("window.end").alias("window_end"),
            "total_units_30d", "defects_30d",
            (F.col("defects_30d") / F.col("total_units_30d")).alias("defect_rate_30d"),
            "avg_vibration_30d", "avg_tool_wear_30d", "avg_solder_thickness_30d",
        )
    )


# =============================================================================
# TABLA FINAL — JOIN de todas las ventanas
# =============================================================================

@dp.table(
    name="gold_machine_aggregations",
    comment=(
        "Métricas de comportamiento por máquina en ventanas de 1h, 24h, 7d y 30d. "
        "Permite detectar degradación progresiva y comparar tasas de defecto actuales "
        "vs baseline histórico."
    ),
    table_properties={"delta.enableChangeDataFeed": "true"},
)
def gold_machine_aggregations():
    h1  = dp.read("agg_1h")
    h24 = dp.read("agg_24h")
    d7  = dp.read("agg_7d")
    d30 = dp.read("agg_30d")

    return (
        h1
        .join(h24, on=["machine_id", "line_id", "window_end"], how="left")
        .join(d7,  on=["machine_id", "line_id", "window_end"], how="left")
        .join(d30, on=["machine_id", "line_id", "window_end"], how="left")
        .withColumn(
            "defect_rate_ratio_1h_vs_30d",
            F.col("defect_rate_1h") / F.nullif(F.col("defect_rate_30d"), F.lit(0))
        )
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )
