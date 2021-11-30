from abc import ABC, abstractmethod
import pandas as pd


class AbstractSynthesizer(ABC):
    @abstractmethod
    def synthesize(self, ori_df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError()


class DummySynthesizer(AbstractSynthesizer):
    def synthesize(self, ori_df: pd.DataFrame) -> pd.DataFrame:
        """Synthesizes the original data (dummy implementation).

        This is not a proper synthesization, just a copy of the input.
        """
        return ori_df.copy()


class DummySynthesizerEmptyResult(AbstractSynthesizer):
    def synthesize(self, ori_df: pd.DataFrame) -> pd.DataFrame:
        """Synthesizes the original data (dummy implementation).

        This is not a proper synthesization, just a copy of the input.
        """
        return pd.DataFrame()
