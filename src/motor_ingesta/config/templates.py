from typing import Annotated, Any, Literal

from databricks.sdk.runtime import dbutils
from pydantic import AliasChoices, BaseModel, ConfigDict, Field, computed_field, model_validator


class BaseConfig(BaseModel):
    """
    Configuración base para las clases de configuración de ingesta.
    Impide que se pasen parámetros adicionales no definidos en el modelo y habilita la validación por nombre y alias.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


# Formatos soportados por la ingesta de archivos
FileFormat = Literal[
    "csv",
    "json",
    "avro",
    "parquet",
    "binaryFile",
]

# Modos de evolución de esquema soportados por la ingesta de archivos
SchemaEvolutionMode = Literal[
    "addNewColumns",
    "addNewColumnsWithTypeWidening",
    "rescue",
    "failOnNewColumns",
    "none",
]


class FileSourceConfig(BaseConfig):
    """
    Esquema para la configuración de la ingesta de archivos desde landing a bronze.
    Permite definir el formato de los archivos, las opciones de lectura, el esquema y la ubicación de los archivos en landing.
    """

    format: FileFormat

    options: dict[str, Any] = Field(default_factory=dict)

    # BaseModel ya tiene un método llamado "schema", para no sobreescribirlo
    # definimos el atributo como "schema_" con "schema" como alias
    schema_: str | None = Field(default=None, alias="schema")
    schema_hints: str | None = Field(default=None, alias="schemaHints")
    schema_evolution_mode: SchemaEvolutionMode | None = Field(default=None, alias="schemaEvolutionMode")
    schema_location: bool = Field(default=False, alias="schemaLocation")

    path: str | None

    @model_validator(mode="after")
    def validate_schema(self):
        """
        Valida que no se pasen parámetros incompatibles entre sí en la configuración de ingesta de archivos.
        """
        if self.schema_ and (self.schema_hints or self.schema_location):
            if self.schema_hints and self.schema_location:
                raise ValueError("'schema' no es compatible con 'schema_hints' y 'schema_location'")
            elif self.schema_hints:
                raise ValueError("'schema' no es compatible con 'schema_hints'")
            else:
                raise ValueError("'schema' no es compatible con 'schema_location'")

        return self

    @computed_field
    @property
    def get_options(self) -> dict[str, Any]:
        """
        Construye un diccionario con todas las opciones de lectura de archivos.
        """
        if not self.schema_evolution_mode and not self.schema_hints:
            return self.options

        options = {**self.options}

        if self.schema_evolution_mode:
            options.update({"cloudFiles.schemaEvolutionMode": self.schema_evolution_mode})
        if self.schema_location:
            options.update({"cloudFiles.schemaHints": self.schema_hints})

        return options


# Tipos de ingesta en streaming sopotados
KafkaFormat = Literal["kafka", "event-hubs"]
# Formatos de mensajes soportados
KafkaFormatMessages = Literal["json", "avro"]


# Configuracion del `scope``, `key` para obtener las credenciales de forma segura a través de `dbutils.secrets`
class KafkaSecret(BaseConfig):
    scope: str
    key: str


class KafkaConnectionConfig(BaseConfig):
    bootstrap_servers: str = Field(validation_alias=AliasChoices("bootstrapServers", "servers"))
    security_protocol: str = Field(default="SASL_SSL", validation_alias=AliasChoices("securityProtocol", "protocol"))
    sasl_mechanism: str = Field(default="PLAIN", validation_alias=AliasChoices("saslMechanism", "sasl"))
    password: KafkaSecret = Field(alias="pass")


TEMPLATE_CONNECTION_STRING = 'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{password}";'


class KafkaSourceConfig(BaseConfig):
    """
    Esquema para la configuración de la ingesta desde Kafka.
    Permite definir el bootstrap servers, el topic y otras opciones de Kafka.
    """

    format: KafkaFormat
    messages: KafkaFormatMessages = Field(alias="message")

    options: dict[str, Any] = Field(default_factory=dict)

    connection: KafkaConnectionConfig
    schema_: str | None = Field(default=None, alias="schema")
    topic: str | None = Field(default=None, alias="subscribe")
    topic_pattern: str | None = Field(
        default=None, validation_alias=AliasChoices("pattern", "topicPattern", "subscribe_pattern", "subscribePattern")
    )

    @model_validator(mode="after")
    def validate_topics(self):
        """
        Valida que se haya especificado al menos un 'topic' o un 'topic_pattern'
        """
        if not self.topic and not self.topic_pattern:
            raise ValueError("Se debe especificar al menos 'topic' o 'topic_pattern'")

        return self

    @computed_field
    @property
    def get_topic(self) -> dict[str, Any]:
        """
        Devuelve la opción de subscription, ya sea 'topic' o 'topic_pattern'.
        """
        if self.topic is not None:
            return {"subscribe": self.topic}
        else:
            return {"subscribePattern": self.topic_pattern}

    @computed_field
    @property
    def get_password(self) -> str:
        return dbutils.secrets.get(scope=self.connection.password.scope, key=self.connection.password.key)

    @computed_field
    @property
    def get_options(self) -> dict[str, Any]:
        """
        Construye un diccionario con las opciones de Kafka.
        """
        options = {
            "kafka.bootstrap.servers": self.connection.bootstrap_servers,
            "kafka.sasl.mechanism": self.connection.sasl_mechanism,
            "kafka.security.protocol": self.connection.security_protocol,
            "kafka.sasl.jaas.config": TEMPLATE_CONNECTION_STRING.format(password=self.get_password),
        }

        # Incluye la subscripción efectiva
        options.update(self.get_topic)

        # Actualiza con el resto de las opciones proporcionadas por el usuario
        options.update(self.options)

        return options


# Atajo para definir los triggers de escritura en el sink de bronze desde el archivo de configuración
# Se define con un diccionario con la clave "type" y el valor del tipo de trigger "value"
# "trigger": {
#     "type": "once",
#     "value": "True"
# }
class BoolTrigger(BaseConfig):
    type: Literal["once", "availableNow"]
    value: bool


class StrTrigger(BaseConfig):
    type: Literal["processingTime", "continuous", "realTime"]
    value: str


Trigger = Annotated[BoolTrigger | StrTrigger, Field(discriminator="type")]


class SinkConfig(BaseConfig):
    """
    Esquema para la configuración del sink de bronze.
    Permite definir el nombre de la tabla de bronze y el trigger de escritura.
    """

    name: str
    trigger: Trigger | None = None

    @computed_field
    @property
    def trigger_config(self) -> dict[str, Any] | None:
        """
        Construye un diccionario con la configuración del trigger de escritura tal como la consume spark.
        """
        if self.trigger is None:
            return None

        return {self.trigger.type: self.trigger.value}


SourceConfig = Annotated[FileSourceConfig | KafkaSourceConfig, Field(discriminator="format")]


class IngestConfig(BaseConfig):
    source: SourceConfig
    sink: SinkConfig


# Clases que se importan con "from templates import *"
__all__ = ["IngestConfig"]
