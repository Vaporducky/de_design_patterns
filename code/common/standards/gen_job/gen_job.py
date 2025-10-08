import datetime
import json
from argparse import Namespace
from pathlib import Path
from dataclasses import dataclass

from ulid import ULID

from utilities.pipeline_enums import PipelineType, PipelineVelocity

# ============================================================================
# The current working directory should be within the job directory; all
# references should be relative this directory:
# code/pipelines/generic_namespace
# ============================================================================
BOUND_CONFIGURATION_PATH: str = str(Path.cwd().joinpath("job_configuration", "config.json"))


@dataclass
class GenJob:
    """
    A generic job class which contains metadata related to a pipeline job.
    Its use is centered around dispatching required information given an event:
        - Alerting (details about the job and the step at which it failed)
        - Logs
        - Exchange information between other systems (object storage, image
          registries, databases, etc.)
        - ...

    The class can be extended within the pipeline definition to provide more
    detailed or not included information.
    """

    namespace: str
    job_name: str
    job_args: Namespace
    type: PipelineType
    velocity: PipelineVelocity
    version: str
    environment: str
    id: ULID
    sources = Path("configuration/sources/sources.yaml")
    targets = Path("configuration/sinks/sinks.yaml")

    def __post_init__(self):
        self.id = ULID()
        # TODO: Better strategy
        job_configuration: dict = self._load_config(self.environment)
        self.__dict__ = self.__dict__ | job_configuration

    @staticmethod
    def _load_config(
            environment: str,
            config_path: str = BOUND_CONFIGURATION_PATH
    ) -> dict:
        try:
            f = open(config_path, 'r', encoding='utf-8')
        except FileNotFoundError:
            print(f"Configuration file {config_path} not found.")
            raise FileNotFoundError
        else:
            with f:
                job_configuration: dict = json.load(f)[environment]
                GenJob._dictionary_pprint(job_configuration)

            return job_configuration

    @staticmethod
    def _dictionary_pprint(dictionary: dict):
        print(json.dumps(dictionary, indent=4, default=str))
