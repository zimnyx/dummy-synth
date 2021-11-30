import random
from abc import ABC, abstractmethod
import pandas as pd


class AbstractEvaluator(ABC):
    @abstractmethod
    def evaluate(self, ori_df: pd.DataFrame, syn_df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError()


class RandomEvaluator(AbstractEvaluator):
    def evaluate(self, ori_df: pd.DataFrame, syn_df: pd.DataFrame) -> pd.DataFrame:
        """Evaluate synthetic data (dummy implementation).

        We assume this score is our evaluation (score) of the given synthetic data.
        To evaluate synthetic data, we compare original data (ori_df) to
        synthetic data (syn_df). We would usually return a 0..1 score, for this
        contrived exercise we just return a random number.

        This is a dummy implementation you can use to evaluate synthetic data.
        """
        return pd.DataFrame(
            [
                {
                    "utility score": random.random(),
                    "privacy score": random.random(),
                }
            ]
        )


class ConstantEvaluator(AbstractEvaluator):
    def evaluate(self, ori_df: pd.DataFrame, syn_df: pd.DataFrame) -> pd.DataFrame:
        """Evaluate synthetic data (dummy implementation).

        We assume this score is our evaluation (score) of the given synthetic data.
        To evaluate synthetic data, we compare original data (ori_df) to
        synthetic data (syn_df). We would usually return a 0..1 score, for this
        contrived exercise we just return fixed number.

        This is a dummy implementation you can use to evaluate synthetic data.
        """
        return pd.DataFrame(
            [
                {
                    "utility score": 0,
                    "privacy score": 1,
                }
            ]
        )
