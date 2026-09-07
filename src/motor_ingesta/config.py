from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


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


if __name__ == "__main__":
    import json
    from pathlib import Path

    pwd = Path.cwd()
    with (pwd.parents[1] / "config.json").open("r") as f:
        tables = json.load(f)

    for t in tables:
        source = t.get("source")
        print(FileSourceConfig.model_validate(source), end="\n\n")
