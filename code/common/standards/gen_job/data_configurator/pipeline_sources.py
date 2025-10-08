import json
import yaml
from enum import Enum, unique
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Any, ClassVar, Type

from pyspark.sql.types import StructType


@unique
class DataFormat(str, Enum):
    CSV = "csv"
    PARQUET = "parquet"
    JDBC = "jdbc"
    DELTA = "delta"


@dataclass
class BaseSource(ABC):
    name: str
    format: DataFormat
    source_configuration: dict[str, str]
    schema: StructType
    options: dict[str, str]
    metadata: dict[str, str]

    # Registry of DataFormat → subclass
    _registry: ClassVar[dict[DataFormat, Type["BaseSource"]]] = {}

    @classmethod
    def register(cls, data_format: DataFormat):
        """Class decorator to register subclasses in the loader registry."""
        def _decorate(subclass: Type["BaseSource"]):
            cls._registry[data_format] = subclass
            return subclass

        return _decorate

    @staticmethod
    def schema_from_json_to_struct(schema_path: Path):
        with open(schema_path, 'r') as f:
            schema = json.load(f)

        return StructType.fromJson(schema)

    @staticmethod
    def _configuration_from_yaml(config_path: Path) -> dict[str, Any]:
        with open(config_path, "r") as f:
            configuration = yaml.safe_load(f)

        return configuration

    @classmethod
    def load_all(cls, yaml_path: Path) -> dict[str, "BaseSource"]:
        """
        Read the master YAML and instantiate one source per named block.

        Args:
            yaml_path: Path to the unified sources.yaml.

        Returns:
            A dict mapping each source-name to its BaseSource subclass instance.
        """
        raw = cls._configuration_from_yaml(yaml_path)
        sources: dict[str, BaseSource] = {}

        for source_type, block in raw.items():
            if source_type != "defaults":
                # Validate top level key
                data_format = DataFormat(source_type)
                subclass = cls._registry.get(data_format)

                # Each block is a mapping of unique_name → config
                for name, config in block.items():
                    sources[name] = subclass.from_config(name, config)

        return sources

    @classmethod
    @abstractmethod
    def from_config(cls, name: str, config: dict[str, Any]) -> "BaseSource":
        """Instantiate a concrete Source from its YAML fragment."""


@dataclass(frozen=True)
class BaseSink:
    name: str
    target_configuration: dict[str, str] = field(repr=False)
    schemas: dict[str, StructType] = field(repr=False)
    partitions: Optional[tuple[str]] = None
    options: dict[str, str] = field(default_factory=dict)
    metadata: dict[str, str] = field(default_factory=dict)


@BaseSource.register(DataFormat.CSV)
@dataclass
class CSVSource(BaseSource):
    @classmethod
    def from_config(cls, name: str, config: dict[str, Any]) -> "CSVSource":
        """
        Build a CSVSource from its YAML config.

        Args:
            name: Unique source identifier (the YAML sub-key).
            config: Dict with keys `path`, `schema_file`, `options` (opt), etc.

        Returns:
            CSVSource instance with loaded StructType.
        """
        # Get configuration
        path = config["path"]
        schema_path = config["schema"]
        options = config.get("options", {})
        metadata = config.get("metadata", {})

        # Essential configuration
        source_configuration = {"path": path}
        # Load schema
        schema = cls.schema_from_json_to_struct(schema_path)

        return cls(name=name,
                   format=DataFormat.CSV,
                   source_configuration=source_configuration,
                   schema=schema,
                   options=options,
                   metadata=metadata)
