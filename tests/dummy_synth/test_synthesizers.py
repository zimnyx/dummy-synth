import pytest
import pandas as pd
from dummy_synth.synthesizers import DummySynthesizer, DummySynthesizerEmptyResult


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
def dummy_synhesizer() -> DummySynthesizer:
    return DummySynthesizer()


@pytest.fixture
def dummy_synhesizer_empty_result() -> DummySynthesizerEmptyResult:
    return DummySynthesizerEmptyResult()


def test__dummy_synhesizer__synthesize__returns_input_dataframe(
    dummy_synhesizer, input_data_frame
):
    result = dummy_synhesizer.synthesize(input_data_frame)
    assert result.equals(input_data_frame)


def test__dummy_synhesizer_empty_result__synthesize__returns_empty_dataframe(
    dummy_synhesizer_empty_result, input_data_frame
):
    result = dummy_synhesizer_empty_result.synthesize(input_data_frame)
    assert result.empty
