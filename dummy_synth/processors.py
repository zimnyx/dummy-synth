import logging
import os
from typing import Dict, Optional
import pandas as pd
from dummy_synth.dataframe_io import AbstractDataFrameIO
from dummy_synth.storages import AbstractFileStorage
from dummy_synth.evaluators import AbstractEvaluator
from dummy_synth.synthesizers import AbstractSynthesizer


class DirProcessor:
    """
    Traverse directory recursively and
    for each supported file extension
    calculate & write sythetic data and its evaluation.

    Here's example of constructor params:
    DirProcessor(
        'mydir',
        LocalDirectoryStorage(),
        {
            ".csv": {
                # object used for reading input
                "read": CsvIO(),
                # object used for writing output
                RecursiveDirProcessor.IO_WRAPPERS_OUTPUT_KEY: CsvIO(),
            },
            ".parquet": {
                RecursiveDirProcessor.IO_WRAPPERS_INPUT_KEY: ParquetIO(),
                RecursiveDirProcessor.IO_WRAPPERS_OUTPUT_KEY: ParquetIO(),
            },
        },
        False,
        DummySynthesizer(),
        '.syn',
        RandomEvaluator(),
        '.eval'
    )
    """

    IO_WRAPPERS_READ_KEY = "read"
    IO_WRAPPERS_WRITE_KEY = "write"

    def __init__(
        self,
        directory: str,
        storage: AbstractFileStorage,
        io_wrappers: Dict[str, Dict[str, AbstractDataFrameIO]],
        overwrite=False,
        synthesizer: Optional[AbstractSynthesizer] = None,
        synthesize_suffix: Optional[str] = None,
        evaluator: Optional[AbstractEvaluator] = None,
        evaluate_suffix: Optional[str] = None,
    ):
        self.directory = directory
        self.storage = storage
        self.io_wrappers = io_wrappers
        self.overwrite = overwrite
        self.synthesizer = synthesizer
        self.synthesize_suffix = synthesize_suffix
        self.evaluator = evaluator
        self.evaluate_suffix = evaluate_suffix

    def process(self) -> int:
        """
        Process each supported file in self.directory and return number of files processed.
        """
        count = 0
        for file_path in self.storage.get_files(self.directory):
            if self.process_file(file_path):
                count += 1
        return count

    def process_file(self, file_path: str) -> bool:
        io_for_read = self.get_dataframe_io(file_path, self.IO_WRAPPERS_READ_KEY)
        io_for_write = self.get_dataframe_io(file_path, self.IO_WRAPPERS_WRITE_KEY)
        if io_for_read is None or io_for_write is None:
            # we don't have IO configuration that supports file with this extension
            logging.debug(f"Skipping file {file_path} due to unsupported extension.")
            return False

        logging.debug(f"Processing file {file_path}.")

        ori_df = io_for_read.read(file_path)
        syn_df = None

        if self.synthesizer is not None:
            syn_df = self.synthesize_to_file(file_path, io_for_write, ori_df)

        if self.evaluator is not None:
            if syn_df is None:
                try:
                    syn_df = io_for_read.read(file_path + self.synthesize_suffix)
                except Exception as e:
                    raise Exception(
                        f"Expected file {file_path + self.synthesize_suffix} cannot be read. Evaluation impossible.",
                        e,
                    )
            self.evaluate_to_file(file_path, io_for_write, ori_df, syn_df)
        logging.debug(f"Successfully processed file {file_path}.")
        return True

    def get_dataframe_io(self, file_path: str, in_or_out: str) -> AbstractDataFrameIO:
        try:
            file_extension = os.path.splitext(file_path)[1].lower()
            return self.io_wrappers[file_extension][in_or_out]
        except KeyError:
            return None

    def check_overwrite(self, path: str):
        if not self.overwrite and self.storage.exists(path):
            raise Exception(
                f"Flag overwrite={self.overwrite} and target file {path} already exists."
            )

    def synthesize_to_file(
        self,
        ori_data_file_path: str,
        io_for_write: AbstractDataFrameIO,
        ori_df: pd.DataFrame,
    ) -> pd.DataFrame:
        output_path = ori_data_file_path + self.synthesize_suffix
        self.check_overwrite(output_path)
        syn_df = self.synthesizer.synthesize(ori_df)
        logging.debug(f"Writing synthesize result to {output_path}.")
        io_for_write.write(
            syn_df,
            output_path,
        )
        return syn_df

    def evaluate_to_file(
        self,
        ori_data_file_path: str,
        io_for_write: AbstractDataFrameIO,
        ori_df: pd.DataFrame,
        syn_df: Optional[pd.DataFrame],
    ) -> pd.DataFrame:
        output_path = ori_data_file_path + self.evaluate_suffix
        self.check_overwrite(output_path)
        eval_df = self.evaluator.evaluate(ori_df, syn_df)
        logging.debug(f"Writing evaluate result to {output_path}.")
        io_for_write.write(
            eval_df,
            output_path,
        )
        return eval_df
