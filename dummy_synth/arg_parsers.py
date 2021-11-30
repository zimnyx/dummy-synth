import argparse
from typing import Callable
from dummy_synth.config_utils import BackendType, Backends
from dummy_synth.processors import DirProcessor
from dummy_synth.synthesizers import AbstractSynthesizer
from dummy_synth.evaluators import AbstractEvaluator


class CommandlineArgumentParserFactory:
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
    def get_parser(
        cls,
        supported_backends: Backends,
        get_local_dir_processor: Callable[[argparse.Namespace], DirProcessor],
        get_s3_dir_processor: Callable[[argparse.Namespace], DirProcessor],
        default_synthesizer: AbstractSynthesizer,
        default_evaluator: AbstractEvaluator,
    ) -> argparse.ArgumentParser:
        output_description = "Output data will be written to separate files in same format/location as original data file."

        parser_main = argparse.ArgumentParser(
            description=f"""
                Traverse directory recursively and for each file with supported extension, perform following action(s): synthesize or/and evaluate.
                {output_description}
                """
        )
        parser_main.add_argument("--version", action="version", version="0.1-dummy")
        subparsers = parser_main.add_subparsers(
            dest="command",
            required=True,
            title="available commands",
        )
        # synthesize
        parser_synthesize = subparsers.add_parser(
            "synthesize",
            description=f"""
                Run synthetize on files in local dir.
                {output_description}
                """,
        )
        parser_synthesize.set_defaults(get_processor=get_local_dir_processor)
        cls.add_debug(parser_synthesize)
        cls.add_overwrite(parser_synthesize)
        cls.add_synthesize_suffix(parser_synthesize)
        cls.add_synthesizer(parser_synthesize, supported_backends, default_synthesizer)
        cls.add_dir(parser_synthesize)

        # evaluate
        parser_evaluate = subparsers.add_parser(
            "evaluate",
            description=f"""
                Run evaluate on data & synthesize files from local dir.
                {output_description}
                Makes sense to run only after synthesize command.
                """,
        )
        parser_evaluate.set_defaults(get_processor=get_local_dir_processor)
        cls.add_debug(parser_evaluate)
        cls.add_overwrite(parser_evaluate)
        cls.add_synthesize_suffix(parser_evaluate)
        cls.add_evaluate_suffix(parser_evaluate)
        cls.add_evaluator(parser_evaluate, supported_backends, default_evaluator)
        cls.add_dir(parser_evaluate)

        # synthesize-and-evaluate
        parser_synthesize_and_evaluate = subparsers.add_parser(
            "synthesize-and-evaluate",
            description=f"""
                Run synthetize & evaluate on files in local dir.
                {output_description}
                """,
        )
        parser_synthesize_and_evaluate.set_defaults(
            get_processor=get_local_dir_processor
        )
        cls.add_debug(parser_synthesize_and_evaluate)
        cls.add_overwrite(parser_synthesize_and_evaluate)
        cls.add_evaluate_suffix(parser_synthesize_and_evaluate)
        cls.add_evaluator(
            parser_synthesize_and_evaluate, supported_backends, default_evaluator
        )
        cls.add_synthesize_suffix(parser_synthesize_and_evaluate)
        cls.add_synthesizer(
            parser_synthesize_and_evaluate, supported_backends, default_synthesizer
        )
        cls.add_dir(parser_synthesize_and_evaluate)

        # synthesize-s3
        parser_synthesize_s3 = subparsers.add_parser(
            "synthesize-s3",
            description=f"""
                Run synthesize on files from S3 bucket.
                {output_description}
                For credentials use AWS_SECRET_ACCESS_KEY and AWS_ACCESS_KEY_ID env variables.
                For AWS region use AWS_DEFAULT_REGION env variable.
                """,
        )
        parser_synthesize_s3.set_defaults(get_processor=get_s3_dir_processor)
        cls.add_debug(parser_synthesize_s3)
        cls.add_overwrite(parser_synthesize_s3)
        cls.add_synthesize_suffix(parser_synthesize_s3)
        cls.add_synthesizer(
            parser_synthesize_s3, supported_backends, default_synthesizer
        )
        cls.add_s3_endpoint_url(parser_synthesize_s3)
        cls.add_s3_bucket(parser_synthesize_s3)
        cls.add_dir(parser_synthesize_s3)

        return parser_main
