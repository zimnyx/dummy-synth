"""Synthesizer module."""

import os
import random
import sys

import pandas as pd


def evaluate(ori_df: pd.DataFrame, syn_df: pd.DataFrame) -> pd.DataFrame:
    """Evaluate synthetic data (dummy implementation).

    We assume this score is our evaluation (score) of the given synthetic data.
    To evaluate synthetic data, we compare original data (ori_df) to
    synthetic data (syn_df). We would usually return a 0..1 score, for this
    contrived exercise we just return a random number.

    This is a dummy implementation you can use to evaluate synthetic data.
    """
    return pd.DataFrame({"utility score": random.random(), "privacy score": random.random()})

def synthesize(ori_df: pd.DataFrame) -> pd.DataFrame:
    """Synthesizes the original data (dummy implementation).

    This is not a proper synthesization, just a copy of the input.
    """
    return ori_df.copy()

class Synthesizer:
    """A synthesizer synthesizes the csv files in the given folder."""

    def __init__(self):
        """Initialize a synthesizer."""
        self.folder = sys.argv[1]
        self.current_folder_files = [
            f for f in os.listdir(self.folder)
            if os.path.isfile(os.path.join(self.folder, f)) and (
                f.endswith(".csv") or f.endswith(".parquet"))
        ]

        self.csv_io = CsvIO()
        self.parquet_io = ParquetIO()

    def synthesize(self):
        """Synthesizes the data given to the constructor."""
        self.synthetic_dfs = []

        for f in self.current_folder_files:
            if f.endswith(".csv"):
                df = self.csv_io.read(f)
            elif f.endswith(".parquet"):
                df = self.parquet_io.read(f)
            else:
                raise ValueError(f"File type of {f} not implemented.")

            self.synthetic_dfs.append(synthesize(df))

    def write(self):
        """Write the synthesization to a file."""
        for f, df in zip(self.current_folder_files, self.synthetic_dfs):
            if f.endswith(".csv"):
                self.csv_io.write(df, f + ".syn")
            elif f.endswith(".parquet"):
                self.parquet_io.write(df, f + ".syn")
            else:
                raise ValueError(f"File type of {f} not implemented.")


class CsvIO:
    """Input/ouput from a csv file."""

    def read(self, f: str) -> pd.DataFrame:
        """Read file."""
        return pd.read_csv(f)

    def write(self, df: pd.DataFrame, f: str) -> None:
        """Write the given dataframe to a file."""
        df.to_csv(f, index=False)



class ParquetIO:
    """Input/ouput from a parquet file."""

    def read(self, f: str) -> pd.DataFrame:
        """Read file."""
        return pd.read_parquet(f)

    def write(self, df: pd.DataFrame, f: str) -> None:
        """Write the given dataframe to a file."""
        df.to_parquet(f, index=False)


if __name__ == "__main__":
    synthesizer = Synthesizer()
    synthesizer.synthesize()
    synthesizer.write()
