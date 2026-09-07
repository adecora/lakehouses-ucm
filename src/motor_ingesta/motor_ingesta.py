from databricks.sdk.runtime import spark
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class MotorIngesta:
    def __init__(self, catalog: str, schema: str, location: str = None):
        self.catalog = catalog
        self.schema = schema
        if location is not None:
            self.tables_location = location
            self.schemas_location = f"{location}/_meta/schema"
            self.checkpoints_location = f"{location}/_meta/checkpoints/{self.__class__.__name__}"

        spark.sql(f"USE CATALOG {self.catalog}")
        spark.sql(f"USE SCHEMA {self.schema}")

    def __add_bronze_file_metadata(self, df: DataFrame) -> DataFrame:
        ingestion_metadata = [F.current_timestamp().alias("_ingested_at")] + [
            F.col(f"_metadata.{c}").alias(f"_{c}") for c in df.select(F.col("_metadata.*")).columns
        ]

        return df.select(*ingestion_metadata, "*")

    def __batch_ingestion(self, path: str):
        return spark.readStream.format("cloudFiles").format("cloudFiles.format", "json").load(path)

    def __streaming_ingestion(self):
        pass

    def ingestion(self, format: str, /, config: dict):
        if format in ("csv", "json", "avro", "parquet"):
            self.__batch_ingestion(config.get("path"))
        elif format in ("kafka", "event_hubs"):
            self.__streaming_ingestion()
        else:
            raise Exception(f'El formato "{format}" no está soportado!')
