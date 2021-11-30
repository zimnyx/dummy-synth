import pytest
from dummy_synth.processors import DirProcessor
from dummy_synth.dataframe_io import CsvIO, ParquetIO


@pytest.fixture
def dir_processor_with_io_config() -> DirProcessor:
    return DirProcessor(
        None,
        None,
        {
            ".csv": {
                "read": CsvIO(),
                "write": ParquetIO(),
            },
            ".parquet": {
                "read": ParquetIO(),
                "write": CsvIO(),
            },
        },
        False,
        None,
        None,
        None,
    )


##############################
# Tests for get_dataframe_io()
##############################


def test__dir_processor__get_dataframe_io__returns_expected_io_for_csv_extension(
    dir_processor_with_io_config,
):
    assert isinstance(
        dir_processor_with_io_config.get_dataframe_io("mydir/foo/bar/a.csv", "read"), CsvIO
    )
    assert isinstance(
        dir_processor_with_io_config.get_dataframe_io("mydir/foo/bar/a.csv", "write"), ParquetIO
    )


def test__dir_processor__get_dataframe_io__returns_expected_io_for_csv_extension_uppercase(
    dir_processor_with_io_config,
):
    assert isinstance(
        dir_processor_with_io_config.get_dataframe_io("mydir/foo/bar/a.CSV", "read"), CsvIO
    )
    assert isinstance(
        dir_processor_with_io_config.get_dataframe_io("mydir/foo/bar/a.CSV", "write"), ParquetIO
    )


def test__dir_processor__check_overwrite_doesnt_throw_exception_if_file_does_not_exist(mocker):
    storage = mocker.Mock()
    storage.exists = mocker.Mock(return_value=False)
    dir_processor  = DirProcessor(
        None,
        storage,
        None,
        False,
        None,
        None,
        None,
    )

    # just don't rise excetpion
    dir_processor.check_overwrite('foo/bar/a.csv')


def test__dir_processor__check_overwrite_throws_exception_if_file_exists_and_ovewrite_is_false(mocker):
    storage = mocker.Mock()
    storage.exists = mocker.Mock(return_value=True)
    dir_processor  = DirProcessor(
        None,
        storage,
        None,
        False,
        None,
        None,
        None,
    )

    with pytest.raises(Exception) as e:
        dir_processor.check_overwrite('foo/bar/a.csv')
        assert str(e) == "Flag overwrite=False and target file foo/bar/a.csv already exists."

def test__dir_processor__check_overwrite_doesnt_throw_exception_if_file_exist_and_overwrite_is_true(mocker):
    storage = mocker.Mock()
    storage.exists = mocker.Mock(return_value=True)
    dir_processor  = DirProcessor(
        None,
        storage,
        None,
        True,
        None,
        None,
        None,
    )

    # just don't rise excetpion
    dir_processor.check_overwrite('foo/bar/a.csv')
