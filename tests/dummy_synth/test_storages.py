import pytest
from pathlib import Path
from dummy_synth.storages import LocalDirectoryStorage, S3Storage


@pytest.fixture
def local_dir_storage() -> LocalDirectoryStorage:
    return LocalDirectoryStorage()


@pytest.fixture
def data_dir() -> Path:
    return Path(__file__).parent.parent.joinpath("test_data")


def test__S3Storage__full_path_to_key_name__returns_expected_value(mocker):
    storage = S3Storage(mocker.Mock(), "my_bucket")
    assert (
        storage.full_path_to_key_name("s3://mybucket/full/path.txt") == "full/path.txt"
    )


@pytest.mark.integration_test
def test__local_dir_storage__exists_returns_true_for_existing_file(
    data_dir, local_dir_storage
):
    assert local_dir_storage.exists(data_dir / "mydata/1/1.csv")


@pytest.mark.integration_test
def test__local_dir_storage__exists_returns_false_for_missing_file(
    data_dir, local_dir_storage
):
    assert local_dir_storage.exists(data_dir / "mydata/1/1.NOT_EXISTS") is False


@pytest.mark.integration_test
def test__local_dir_storage__get_files__returns_expected_files_in_generator(
    data_dir, local_dir_storage
):
    files = list(local_dir_storage.get_files(data_dir))
    assert len(files) == 2
    assert str(data_dir / 'mydata/1/1.csv') in files
    assert str(data_dir / 'mydata/1/1.parquet') in files

