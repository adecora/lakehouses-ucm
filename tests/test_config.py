import pathlib

import pytest
from pydantic import ValidationError

from motor_ingesta import ConfigLoader
from motor_ingesta.config.templates import FileSourceConfig, KafkaSourceConfig, Partition

TESTS_DIR = pathlib.Path(__file__).parent / "resources"
CONFIG_JSON = TESTS_DIR / "config.json"
KAFKA_CONFIG_JSON = TESTS_DIR / "kafka-config.json"


def test_config_json_parses_file_sources():
    """
    Valida el parseo del fichero de configuración para los sources de tipo "FileSourceConfig".
    """
    configs = ConfigLoader.from_file(CONFIG_JSON)

    assert len(configs) == 2

    events, orders = configs

    assert isinstance(events.source, FileSourceConfig)
    assert events.source.format == "json"
    assert events.source.schema_evolution_mode == "rescue"
    assert events.sink.name == "events_raw"
    assert events.sink.partition.type == "year_month"
    assert events.sink.trigger_config == {"availableNow": True}

    assert isinstance(orders.source, FileSourceConfig)
    assert orders.source.format == "parquet"
    assert orders.source.schema_evolution_mode == "addNewColumns"
    assert orders.sink.name == "orders_raw"
    assert orders.sink.trigger_config == {"once": True}


def test_kafka_config_json_parses_kafka_sources():
    """
    Valida el parseo del fichero de configuración para los sources de tipo "KafkaSourceConfig".
    """
    configs = ConfigLoader.from_file(KAFKA_CONFIG_JSON)

    assert len(configs) == 2

    sensors_json, sensors_avro = configs

    assert isinstance(sensors_json.source, KafkaSourceConfig)
    assert sensors_json.source.format == "event-hubs"
    assert sensors_json.source.messages == "json"
    assert sensors_json.source.topic == "sensors-json"
    assert sensors_json.source.get_topic == {"subscribe": "sensors-json"}
    assert sensors_json.sink.trigger_config == {"availableNow": True}

    assert isinstance(sensors_avro.source, KafkaSourceConfig)
    assert sensors_avro.source.messages == "avro"
    assert sensors_avro.source.topic_pattern == "sensors-a.*"
    assert sensors_avro.source.get_topic == {"subscribePattern": "sensors-a.*"}
    assert sensors_avro.sink.trigger_config == {"once": True}


def test_config_loader_from_dict():
    """
    Valida que el ConfigLoader pueda parsear correctamente un diccionario de configuración.
    """
    data = {
        "source": {"format": "csv", "path": "clientes_??.csv"},
        "sink": {"name": "clientes_raw"},
    }

    configs = ConfigLoader.from_dict(data)

    assert len(configs) == 1
    assert configs[0].source.format == "csv"
    assert configs[0].sink.name == "clientes_raw"


def test_config_loader_from_list():
    """
    Valida que el ConfigLoader pueda parsear correctamente una lista de configuraciones.
    """
    data = [
        {"source": {"format": "csv", "path": "a.csv"}, "sink": {"name": "a_raw"}},
        {"source": {"format": "json", "path": "b.json"}, "sink": {"name": "b_raw"}},
    ]

    configs = ConfigLoader.from_list(data)

    assert [c.sink.name for c in configs] == ["a_raw", "b_raw"]


def test_config_loader_parse_rejects_unsupported_type():
    """
    Valida que el ConfigLoader lance un TypeError al intentar parsear un tipo no soportado.
    """
    with pytest.raises(TypeError):
        ConfigLoader.parse("not-a-dict-or-list")


def test_file_source_config_rejects_schema_and_schema_hints():
    """
    Valida que FileSourceConfig lance un ValidationError si se proporcionen tanto 'schema' como 'schemaHints'.
    """
    with pytest.raises(ValidationError):
        FileSourceConfig(
            format="csv",
            path="a.csv",
            schema="id int",
            schemaHints="id int",
        )


def test_partition_column_requires_column_name():
    """
    Valida que Partition lance un ValidationError si se especifica 'type="column"' sin proporcionar 'column'.
    """
    with pytest.raises(ValidationError):
        Partition(type="column")


def test_partition_year_rejects_column_name():
    """
    Valida que Partition lance un ValidationError si se especifica 'type="year"' junto con 'column'.
    """
    with pytest.raises(ValidationError):
        Partition(type="year", column="id")


def test_kafka_source_config_requires_topic_or_pattern():
    """
    Valida que KafkaSourceConfig lance un ValidationError si no se proporciona 'topic' ni 'topic_pattern'.
    """
    with pytest.raises(ValidationError):
        KafkaSourceConfig(
            format="event-hubs",
            message="json",
            connection={
                "bootstrapServers": "host:9093",
                "pass": {"scope": "scope", "key": "key"},
            },
        )
