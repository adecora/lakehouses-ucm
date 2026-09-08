from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, computed_field, model_validator


class BaseConfig(BaseModel):
    """
    Set de la configuración para no permitir argumentos extra
    """

    model_config = ConfigDict(extra="forbid", validate_by_name=True, validate_by_alias=True)


FileFormat = Literal[
    "csv",
    "json",
    "avro",
    "parquet",
    "binaryFile",
]

SchemaEvolutionMode = Literal[
    "addNewColumns",
    "addNewColumnsWithTypeWidening",
    "rescue",
    "failOnNewColumns",
    "none",
]


class FileSourceConfig(BaseConfig):
    """
    Esquema para la ingesta de ficheros
    """

    format: FileFormat

    options: dict[str, Any] | None = Field(default_factory=dict)

    # BaseModel ya tiene un método llamado "schema", para no sobreescribirlo
    # definimos el atributo como "schema_" con "schema" como alias
    schema_: str | None = Field(default=None, alias="schema")
    schema_hints: str | None = Field(default=None, alias="schemaHints")
    schema_evolution_mode: SchemaEvolutionMode | None = Field(default=None, alias="schemaEvolutionMode")
    schema_location: str | None = Field(default=None, alias="schemaLocation")

    path: str | None

    @model_validator(mode="after")
    def validate_schema(self):
        if self.schema_ and (self.schema_hints or self.schema_location):
            if self.schema_hints and self.schema_location:
                raise ValueError("'schema' no es compatible con 'schema_hints' y 'schema_location'")
            elif self.schema_hints:
                raise ValueError("'schema' no es compatible con 'schema_hints'")
            else:
                raise ValueError("'schema' no es compatible con 'schema_location'")

        return self


class BoolTrigger(BaseConfig):
    type: Literal["once", "availableNow"]
    value: bool


class StrTrigger(BaseConfig):
    type: Literal["processingTime", "continuous", "realTime"]
    value: str


Trigger = Annotated[BoolTrigger | StrTrigger, Field(discriminator="type")]


class SinkConfig(BaseConfig):
    """
    Esquema para el volcado de datos
    """

    name: str
    trigger: Trigger | None = None

    @computed_field
    @property
    def trigger_config(self) -> str | None:
        if self.trigger is None:
            return None

        return {self.trigger.type: self.trigger.value}


class IngestConfig(BaseConfig):
    source: FileSourceConfig
    sink: SinkConfig


__all__ = ["IngestConfig"]

if __name__ == "__main__":
    import json
    from pathlib import Path

    pwd = Path.cwd()
    with (pwd.parents[1] / "config.json").open("r") as f:
        tables = json.load(f)

    for t in tables:
        so = t.get("source")
        si = t.get("sink")

        print(IngestConfig.model_validate(t), end="\n\n")

    print(IngestConfig.model_validate_json((pwd.parents[1] / "config.json").read_text()))
