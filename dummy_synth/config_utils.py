from copy import deepcopy
from enum import Enum
from typing import Any, Dict, List


class BackendType(Enum):
    DATAFRAME_IO = "dataframe_io"
    STORAGE = "storage"
    SYNTHESIZER = "synthesizer"
    EVALUATOR = "evaluator"


class Backends:
    """
    Utility class that handles instantiation of backends of different types.

    Here's example of constructor args:
    backends = Backends({
        BackendType.DATAFRAME_IO: {
            "csv_default": CsvIO,
            "parquet_default": ParquetIO,
        },
        BackendType.STORAGE: {
            "LocalDirectoryStorage": LocalDirectoryStorage,
            "S3Storage": S3Storage,
        },
        BackendType.SYNTHESIZER: {
            "DummySynthesizer": DummySynthesizer,
            "DummySynthesizerEmptyResult": DummySynthesizerEmptyResult,
        },
        BackendType.EVALUATOR: {
            "RandomEvaluator": RandomEvaluator,
            "ConstantEvaluator": ConstantEvaluator,
        }
    })

    """

    def __init__(self, backends: Dict[BackendType, Dict[str, Any]]):
        self.backends = backends

    def get_supported_backends(self, backend_type: BackendType) -> List[str]:
        return self.backends[backend_type].keys()

    def get_backed_instance(
        self,
        backend_type: BackendType,
        backend_name: str,
        *backend_args,
        **backend_kwargs
    ) -> Any:
        backend_class = self.backends[backend_type][backend_name]
        if backend_class is None:
            return None
        return backend_class(*backend_args, **backend_kwargs)

    def get_raw_config(self, backend_type: BackendType) -> Dict[str, Any]:
        return self.backends[backend_type]


def prepare_processor_dataframe_io_config(
    backends: Backends,
    config: Dict[str, Dict[str, str]],
    *backend_args,
    **backend_kwargs
) -> Dict[str, Dict[str, Any]]:
    """
    Return config with DataFrame IO backend names converted to instances.
    """
    config = deepcopy(config)
    for file_extension, extension_config in config.items():
        for read_or_write, backend_name in extension_config.items():
            extension_config[read_or_write] = backends.get_backed_instance(
                BackendType.DATAFRAME_IO, backend_name, *backend_args, **backend_kwargs
            )
    return config
