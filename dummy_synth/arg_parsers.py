import argparse
from functools import partial
from dummy_synth.config_utils import BackendType, Backends
from dummy_synth.processors import DirProcessor
from dummy_synth.synthesizers import AbstractSynthesizer
from dummy_synth.evaluators import AbstractEvaluator
import boto3
from local_config import (
    RECURSIVE_DIR_PROCESSOR_CONFIG,
    DEFAULT_SYNTHESIZE_SUFFIX,
    DEFAULT_EVALUATE_SUFFIX,
)
from dummy_synth.config_utils import (
    prepare_processor_dataframe_io_config,
)


class CommandlineArgumentParserFactory:
    """
    Factory for commandline parser providing evaluate and synthesze commands.
    """

    output_description = "Output data will be written to separate files in same format/location as original data file."

    @classmethod
    def get_parser(
        cls,
        supported_backends: Backends,
        default_synthesizer: AbstractSynthesizer,
        default_evaluator: AbstractEvaluator,
    ) -> argparse.ArgumentParser:

        parser_main = argparse.ArgumentParser(
            description=f"""
                Traverse directory recursively and for each file with supported extension, perform following action(s): synthesize or evaluate.
                {cls.output_description}
                """
        )
        parser_main.add_argument("--version", action="version", version="0.1-dummy")
        subparsers = parser_main.add_subparsers(
            dest="command",
            required=True,
            title="available commands",
        )
        # synthesize
        cls.setup_synthezise_parser(supported_backends, default_synthesizer, subparsers)

        # evaluate
        cls.setup_evaluate_parser(supported_backends, default_evaluator, subparsers)

        # synthesize-s3
        cls.setup_synthezise_s3_parser(
            supported_backends, default_synthesizer, subparsers
        )

        return parser_main

    @classmethod
    def get_basic_processor_kwargs(
        cls, backends: Backends, args: argparse.Namespace
    ) -> DirProcessor:
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

    @classmethod
    def get_local_dir_processor(
        cls, backends: Backends, args: argparse.Namespace
    ) -> DirProcessor:
        processor_kwargs = cls.get_basic_processor_kwargs(backends, args)
        processor_kwargs["storage"] = backends.get_backed_instance(
            BackendType.STORAGE, "LocalDirectoryStorage"
        )
        processor_kwargs["io_wrappers"] = prepare_processor_dataframe_io_config(
            backends, RECURSIVE_DIR_PROCESSOR_CONFIG
        )
        return DirProcessor(**processor_kwargs)

    @classmethod
    def get_s3_dir_processor(
        cls, backends: Backends, args: argparse.Namespace
    ) -> DirProcessor:
        s3_endpoint_config = {"endpoint_url": args.s3_endpoint_url}
        s3 = boto3.resource("s3", **s3_endpoint_config)
        processor_kwargs = cls.get_basic_processor_kwargs(backends, args)
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

    @classmethod
    def add_debug(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--debug",
            action="store_true",
            default=False,
            help="verbose debugging output",
        )

    @classmethod
    def add_overwrite(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--overwrite",
            action="store_true",
            default=False,
            help="overwrite output files if exist (default is not overwrite)",
        )

    @classmethod
    def add_synthesize_suffix(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--synthesize-suffix",
            help="use this suffix when writing synthesize file",
        )

    @classmethod
    def add_synthesizer(
        cls,
        parser: argparse.ArgumentParser,
        supported_backends: Backends,
        default_synthesizer: AbstractSynthesizer,
    ) -> None:
        parser.add_argument(
            "--synthesizer",
            choices=supported_backends.get_supported_backends(BackendType.SYNTHESIZER),
            help=f"synthesizer name (default: {default_synthesizer})",
            default=default_synthesizer,
        )

    @classmethod
    def add_evaluate_suffix(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--evaluate-suffix",
            help="use this suffix when writing evaluate file",
        )

    @classmethod
    def add_evaluator(
        cls,
        parser: argparse.ArgumentParser,
        supported_backends: Backends,
        default_evaluator: AbstractEvaluator,
    ) -> None:
        parser.add_argument(
            "--evaluator",
            choices=supported_backends.get_supported_backends(BackendType.EVALUATOR),
            help=f"evaluator name (default: {default_evaluator})",
            default=default_evaluator,
        )

    @classmethod
    def add_dir(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("dir", help="directory to be traversed")

    @classmethod
    def add_s3_endpoint_url(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--s3-endpoint-url",
            help="use this if you want to connect with self-hosted S3 service (localstack, MinIO)",
        )

    @classmethod
    def add_s3_bucket(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("s3_bucket", help="S3 bucket")

    @classmethod
    def setup_synthezise_parser(
        cls,
        supported_backends: Backends,
        default_synthesizer: AbstractSynthesizer,
        subparsers: argparse._SubParsersAction,
    ) -> None:
        parser = subparsers.add_parser(
            "synthesize",
            description=f"""
                Run synthetize on files in local dir.
                {cls.output_description}
                """,
        )
        parser.set_defaults(
            get_processor=partial(cls.get_local_dir_processor, supported_backends)
        )
        cls.add_debug(parser)
        cls.add_overwrite(parser)
        cls.add_synthesize_suffix(parser)
        cls.add_synthesizer(parser, supported_backends, default_synthesizer)
        cls.add_dir(parser)

    @classmethod
    def setup_synthezise_s3_parser(
        cls,
        supported_backends: Backends,
        default_synthesizer: AbstractSynthesizer,
        subparsers: argparse._SubParsersAction,
    ) -> None:
        parser = subparsers.add_parser(
            "synthesize-s3",
            description=f"""
                Run synthesize on files from S3 bucket.
                {cls.output_description}
                For credentials use AWS_SECRET_ACCESS_KEY and AWS_ACCESS_KEY_ID env variables.
                For AWS region use AWS_DEFAULT_REGION env variable.
                """,
        )
        parser.set_defaults(
            get_processor=partial(cls.get_s3_dir_processor, supported_backends)
        )
        cls.add_debug(parser)
        cls.add_overwrite(parser)
        cls.add_synthesize_suffix(parser)
        cls.add_synthesizer(parser, supported_backends, default_synthesizer)
        cls.add_s3_endpoint_url(parser)
        cls.add_s3_bucket(parser)
        cls.add_dir(parser)

    @classmethod
    def setup_evaluate_parser(
        cls,
        supported_backends: Backends,
        default_evaluator: AbstractEvaluator,
        subparsers: argparse._SubParsersAction,
    ) -> None:
        parser = subparsers.add_parser(
            "evaluate",
            description=f"""
                Run evaluate on data & synthesize files from local dir.
                {cls.output_description}
                Makes sense to run only after synthesize command.
                """,
        )
        parser.set_defaults(
            get_processor=partial(cls.get_local_dir_processor, supported_backends)
        )
        cls.add_debug(parser)
        cls.add_overwrite(parser)
        cls.add_synthesize_suffix(parser)
        cls.add_evaluate_suffix(parser)
        cls.add_evaluator(parser, supported_backends, default_evaluator)
        cls.add_dir(parser)
