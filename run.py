import logging
import sys
from dummy_synth.arg_parsers import CommandlineArgumentParserFactory
from local_config import (
    BACKENDS,
    DEFAULT_SYNTHESIZER,
    DEFAULT_EVALUATOR,
)
from dummy_synth.config_utils import (
    Backends,
)

"""
Command line tools for data synthesization and evaluation.
"""


if __name__ == "__main__":
    # backends for IO & storage
    backends = Backends(BACKENDS)
    args = CommandlineArgumentParserFactory.get_parser(
        backends,
        DEFAULT_SYNTHESIZER,
        DEFAULT_EVALUATOR,
    ).parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)

    try:
        files_count = args.get_processor(args).process()
    except Exception as e:
        # logging.exception(e)
        print(f"Stopping due to error: {e}")
        sys.exit(1)
    print(f"Files processed: {files_count}")
