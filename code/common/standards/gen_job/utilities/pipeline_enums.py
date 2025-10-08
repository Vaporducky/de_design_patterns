from enum import Enum, auto, unique


@unique
class PipelineType(str, Enum):
    def _generate_next_value_(name, start, count, last_values):
        return name.lower()

    INGESTION: str = auto()
    TRANSFORMATION: str = auto()


@unique
class PipelineVelocity(str, Enum):
    def _generate_next_value_(name, start, count, last_values):
        return name.lower()

    BATCH: str = auto()
    STREAM: str = auto()
