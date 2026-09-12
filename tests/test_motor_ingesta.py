"""
Los test unitarios para la clase MotorIngesta.
Estos test utilizan
"""

from pyspark.sql import SparkSession

from motor_ingesta import MotorIngesta

# Valores por defecto del catálogo y esquema para los tests
# Se usa el shema "default" para los tests unitarios
CATALOG = "adbmade01alvare05"
SCHEMA = "default"


def test_motor_ingesta_sets_catalog_and_schema(spark: SparkSession):
    """
    Valida que MotorIngesta configure correctamente el catálogo y el esquema actuales en Spark.
    """
    motor = MotorIngesta(
        catalog=CATALOG,
        schema=SCHEMA,
        landing="/tmp/motor_ingesta_test/landing",
        meta="/tmp/motor_ingesta_test/meta",
    )

    current_catalog = spark.sql("SELECT current_catalog()").first()[0]
    current_schema = spark.sql("SELECT current_schema()").first()[0]

    assert current_catalog == CATALOG
    assert current_schema == SCHEMA
    assert motor.tables_location is None


def test_motor_ingesta_computes_metadata_paths(spark: SparkSession):
    """
    Valida que MotorIngesta calcule correctamente las rutas de metadatos (schemas, checkpoints y tablas).
    """
    meta = "/tmp/motor_ingesta_test/meta"

    motor = MotorIngesta(
        catalog=CATALOG,
        schema=SCHEMA,
        landing="/tmp/motor_ingesta_test/landing",
        meta=meta,
        location="abfss://lakehouse@example.dfs.core.windows.net/farmia/bronze",
    )

    assert motor.schemas_location == f"{meta}/schemas/MotorIngesta"
    assert motor.checkpoints_location == f"{meta}/checkpoints/MotorIngesta"
    assert motor.tables_location == "abfss://lakehouse@example.dfs.core.windows.net/farmia/bronze"
