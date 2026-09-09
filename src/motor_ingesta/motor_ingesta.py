from databricks.sdk.runtime import spark
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from .config import IngestConfig
from .config.templates import FileSourceConfig, SinkConfig


class MotorIngesta:
    """
    Motor de ingesta para consumir datos de landing en distintos formatos e ingestarlos en bronze.
    """

    _file_ingestion = ("csv", "json", "avro", "parquet", "binaryFile")
    _stream_ingestion = ("kafka", "event_hubs")

    def __init__(self, catalog: str, schema: str, landing: str, meta: str, location: str | None = None) -> None:
        """
        Inicializa el motor de ingesta con la configuración de catálogo, esquema, landing, meta y ubicación de tablas.

        Parámetros:
        ===========
        - catalog: Nombre del catálogo de Databricks donde se van a guardar las tablas de bronze.
        - schema: Nombre del esquema de Databricks donde se van a guardar las tablas de bronze.
        - landing: Ruta de landing donde se encuentran los archivos a ingerir.
        - meta: Ruta donde se almacenan los metadatos de ingesta. Se generan dos subcarpetas dentro de esta ruta:
            - /schemas/MotorIngesta: Para almacenar los esquemas de los archivos ingeridos.
            - /checkpoints/MotorIngesta: Para almacenar los checkpoints de las consultas de ingesta.
        - location: Ruta en caso que se requira almacenar las tablas como tablas externas, en caso contrario se almacenarán
        como tablas manejadas por Databricks en el esquema especificado.
        """
        meta_dir = self.__class__.__name__
        self.catalog = catalog
        self.schema = schema

        self.staging_location = landing
        self.schemas_location = f"{meta}/schemas/{meta_dir}"
        self.checkpoints_location = f"{meta}/checkpoints/{meta_dir}"
        self.tables_location = location

        spark.sql(f"USE CATALOG {self.catalog}")
        spark.sql(f"USE SCHEMA {self.schema}")

    def __add_bronze_file_metadata(self, df: DataFrame) -> DataFrame:
        """
        Agrega metadatos de ingesta a un DataFrame de bronze, incluyendo la fecha de ingesta y los metadatos del archivo.
        """
        ingestion_metadata = [F.current_timestamp().alias("_ingested_at")] + [
            F.col(f"_metadata.{c}").alias(f"_{c}") for c in df.select(F.col("_metadata.*")).columns
        ]

        return df.select(*ingestion_metadata, "*")

    def __file_ingestion(self, source: FileSourceConfig, *, table_name: str) -> DataFrame:
        """
        Ingesta de archivos desde landing a bronze.

        Parámetros:
        ===========
        - source: Configuración de la fuente de datos a ingerir.
        - table_name: Nombre de la tabla de bronze donde se van a almacenar. Crea un subdirectorio para almacenar el esquema en caso que sea necesario.
        """
        reader = spark.readStream.format("cloudFiles").option("cloudFiles.format", source.format)

        if source.schema_:
            reader = reader.schema(source.schema_)

        if source.schema_location:
            reader = reader.option("cloudfiles.schemaLocation", f"{self.schemas_location}/{table_name}")

        return (
            reader.options(**source.options)
            .load(f"{self.staging_location}/{source.path}")
            .transform(self.__add_bronze_file_metadata)
        )

    def __write_stream(self, df: DataFrame, sink: SinkConfig, *, trigger: dict | None = None) -> StreamingQuery:
        """
        Escribe un DataFrame en un sink de bronze.

        Parámetros:
        ===========
        - df: DataFrame a escribir en el sink.
        - sink: Configuración del sink de bronze donde se va a escribir el DataFrame.
        - trigger: Configuración del trigger de escritura. Si no se especifica, se utilizará el trigger por defecto de Databricks.
        Ver: https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/triggers
        """
        table_name = sink.name

        if sink.trigger is not None:
            trigger = sink.trigger_config

        if self.tables_location:
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {table_name}
                        USING DELTA
                        LOCATION '{self.tables_location}/{table_name}'
                    """)
        writer = df.writeStream.option("checkpointLocation", f"{self.checkpoints_location}/{table_name}")

        if trigger is not None:
            writer = writer.trigger(**trigger)

        return writer.toTable(table_name)

    def ingest(self, config: IngestConfig):
        """
        Ingesta de datos desde landing a bronze. Utiliza la configuración de ingesta para determinar el origen y destino de los datos.
        """
        queries = []

        for ingestion in config:
            source = ingestion.source
            sink = ingestion.sink

            if source.format in self._file_ingestion:
                df = self.__file_ingestion(source, table_name=sink.name)
                query = self.__write_stream(df, sink, trigger={"availableNow": True})
                queries.append(query)

            elif source.format in self._stream_ingestion:
                pass
            else:
                raise Exception(f'El formato "{format}" no está soportado!')

        return queries
