import argparse
import logging
import sys
import boto3
from dummy_synth.arg_parsers import CommandlineArgumentParserFactory
from dummy_synth.processors import DirProcessor
from local_config import (
    BACKENDS,
    RECURSIVE_DIR_PROCESSOR_CONFIG,
    DEFAULT_SYNTHESIZER,
    DEFAULT_EVALUATOR,
    DEFAULT_SYNTHESIZE_SUFFIX,
    DEFAULT_EVALUATE_SUFFIX,
)
from dummy_synth.config_utils import (
    BackendType,
    Backends,
    prepare_processor_dataframe_io_config,
)


def get_basic_processor_kwargs(args: argparse.Namespace) -> DirProcessor:
    processor_kwargs = {
        "directory": args.dir,
        "overwrite": args.overwrite,
    }
    if "synthesizer" in args:
        processor_kwargs["synthesizer"] = backends.get_backed_instance(
            BackendType.SYNTHESIZER, args.synthesizer
        )
        processor_kwargs["synthesize_suffix"] = (
            args.synthesize_suffix or DEFAULT_SYNTHESIZE_SUFFIX
        )

    if "evaluator" in args:
        processor_kwargs["evaluator"] = backends.get_backed_instance(
            BackendType.EVALUATOR, args.evaluator
        )
        processor_kwargs["synthesize_suffix"] = (
            args.synthesize_suffix or DEFAULT_SYNTHESIZE_SUFFIX
        )
        processor_kwargs["evaluate_suffix"] = (
            args.evaluate_suffix or DEFAULT_EVALUATE_SUFFIX
        )
    return processor_kwargs


def get_local_dir_processor(args: argparse.Namespace) -> DirProcessor:
    processor_kwargs = get_basic_processor_kwargs(args)
    processor_kwargs["storage"] = backends.get_backed_instance(
        BackendType.STORAGE, "LocalDirectoryStorage"
    )
    processor_kwargs["io_wrappers"] = prepare_processor_dataframe_io_config(
        backends, RECURSIVE_DIR_PROCESSOR_CONFIG
    )
    return DirProcessor(**processor_kwargs)


def get_s3_dir_processor(args: argparse.Namespace) -> DirProcessor:
    s3_endpoint_config = {"endpoint_url": args.s3_endpoint_url}
    s3 = boto3.resource("s3", **s3_endpoint_config)
    processor_kwargs = get_basic_processor_kwargs(args)
    processor_kwargs["storage"] = backends.get_backed_instance(
        BackendType.STORAGE, "S3Storage", s3, args.s3_bucket
    )
    processor_kwargs["io_wrappers"] = prepare_processor_dataframe_io_config(
        backends,
        RECURSIVE_DIR_PROCESSOR_CONFIG,
        # pandas will use this for s3fs config
        storage_options={
            "client_kwargs": s3_endpoint_config,
        },
    )
    return DirProcessor(**processor_kwargs)


if __name__ == "__main__":
    backends = Backends(BACKENDS)
    args = CommandlineArgumentParserFactory.get_parser(
        backends,
        get_local_dir_processor,
        get_s3_dir_processor,
        DEFAULT_SYNTHESIZER,
        DEFAULT_EVALUATOR,
    ).parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)

    try:
        files_count = args.get_processor(args).process()
    except Exception as e:
        #logging.exception(e)
        print(f"Stopping due to error: {e}")
        sys.exit(1)
    print(f"Files processed: {files_count}")
