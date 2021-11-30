from abc import ABC, abstractmethod
import pandas as pd


class AbstractDataFrameIO(ABC):
    @abstractmethod
    def read(self, source: str, **kwargs) -> pd.DataFrame:
        """Read source into DataFrame"""
        raise NotImplementedError()

    @abstractmethod
    def write(self, df: pd.DataFrame, target: str, **kwargs) -> None:
        """Write DataFrame to target"""
        raise NotImplementedError()


class CsvIO(AbstractDataFrameIO):
    """Input/ouput from a csv file."""

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def read(self, source: str) -> pd.DataFrame:
        return pd.read_csv(source, **self.kwargs)

    def write(self, df: pd.DataFrame, target: str) -> None:
        df.to_csv(target, index=False, **self.kwargs)


class ParquetIO(AbstractDataFrameIO):
    """Input/ouput from a parquet file."""

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def read(self, source: str) -> pd.DataFrame:
        return pd.read_parquet(source, **self.kwargs)

    def write(self, df: pd.DataFrame, target) -> None:
        df.to_parquet(target, index=False, **self.kwargs)
