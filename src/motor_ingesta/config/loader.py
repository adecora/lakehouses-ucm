import json
from pathlib import Path
from typing import Any

from pydantic import TypeAdapter

from .templates import IngestConfig


class ConfigLoader:
    """
    Loader de configuración para la ingesta de datos desde landing a bronze.
    Permite cargar la configuración desde un diccionario, una lista de diccionarios o un archivo JSON.
    """

    _list_ingestion = TypeAdapter(list[IngestConfig])

    @classmethod
    def from_dict(cls, data: dict[str:Any]) -> list[IngestConfig]:
        return [IngestConfig.model_validate(data)]

    @classmethod
    def from_list(cls, data: list[dict[str:Any]]) -> list[IngestConfig]:
        return cls._list_ingestion.validate_python(data)

    @classmethod
    def from_file(cls, path: str | Path) -> list[IngestConfig]:

        path = Path(path)
        data = json.loads(path.read_text())

        return cls.parse(data)

    @classmethod
    def parse(cls, data: dict[str:Any] | list[dict[str:Any]]) -> list[IngestConfig]:

        if isinstance(data, dict):
            return cls.from_dict(data)

        if isinstance(data, list):
            return cls.from_list(data)

        raise TypeError(f"Tipo de configuración no soportado: {type(data).__name__}")


# Clases que se importan con "from loader import *"
__all__ = ["ConfigLoader"]

if __name__ == "__main__":
    import json
    from pathlib import Path

    dir = Path(__file__).parents[3] / "config.json"

    tables = json.loads(dir.read_text())

    for t in tables:
        print(t, end="\n\n")
        print(ConfigLoader.from_dict(t))

    print("\n=================================\n")

    print(ConfigLoader.from_list(tables))

    print("\n=================================\n")

    print(ConfigLoader.from_file(dir))
