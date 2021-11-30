import pytest
import pandas as pd
from dummy_synth.evaluators import ConstantEvaluator, RandomEvaluator


@pytest.fixture
def input_data_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "col1": "A",
                "col2": "B",
            },
            {
                "col1": "C",
                "col2": "D",
            },
        ]
    )


@pytest.fixture
def random_evaluator() -> RandomEvaluator:
    return RandomEvaluator()


@pytest.fixture
def constant_evaluator() -> ConstantEvaluator:
    return ConstantEvaluator()


def test__random_evaluator__evaluate__returns_expected_value(
    random_evaluator, input_data_frame
):
    result = random_evaluator.evaluate(input_data_frame, input_data_frame)
    assert len(result) == 1
    assert list(result.columns) == ["utility score", "privacy score"]
    assert 0 <= result["utility score"][0] <= 1
    assert 0 <= result["privacy score"][0] <= 1


def test__constant_evaluator__evaluate__returns_expected_value(
    constant_evaluator, input_data_frame
):
    result = constant_evaluator.evaluate(input_data_frame, input_data_frame)
    assert len(result) == 1
    assert list(result.columns) == ["utility score", "privacy score"]
    assert result["utility score"][0] == 0
    assert result["privacy score"][0] == 1
