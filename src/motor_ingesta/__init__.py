import importlib.metadata

# Ver: https://docs.astral.sh/ruff/linter/#file-level
# Ver: https://docs.astral.sh/ruff/rules/unsorted-imports/
# ruff: noqa: I001
from .config import IngestConfig, ConfigLoader
from .motor_ingesta import MotorIngesta

__version__ = importlib.metadata.version("motor_ingesta")
__author__ = importlib.metadata.metadata("motor_ingesta")["Author"]

__all__ = ["MotorIngesta", "IngestConfig", "ConfigLoader"]


def get_version() -> str:
    """
    Devuelve la versión del paquete motor_ingesta.
    :return: Versión del paquete motor_ingesta.
    """
    return importlib.metadata.version("motor_ingesta")
