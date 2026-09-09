from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, computed_field, model_validator


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

    options: dict[str, Any] | None = Field(default_factory=dict)

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
    def get_options(self) -> str | None:
        """
        Construye un diccionario con todas las opciones de lectura de archivos.
        """
        if self.schema_evolution_mode or self.schema_hints:
            return self.options

        options = {**self.options}

        if self.schema_evolution_mode:
            options.update({"cloudFiles.schemaEvolutionMode": self.schema_evolution_mode})
        if self.schema_location:
            options.update({"cloudFiles.schemaHints": self.schema_hints})

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
    def trigger_config(self) -> str | None:
        """
        Construye un diccionario con la configuración del trigger de escritura tal como la consume spark.
        """
        if self.trigger is None:
            return None

        return {self.trigger.type: self.trigger.value}


class IngestConfig(BaseConfig):
    source: FileSourceConfig
    sink: SinkConfig


# Clases que se importan con "from templates import *"
__all__ = ["IngestConfig"]
