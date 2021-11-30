import os
from typing import Generator
from abc import ABC, abstractmethod
import boto3
import botocore


class AbstractFileStorage(ABC):
    @abstractmethod
    def get_files(self, directory: str) -> Generator[str, None, None]:
        raise NotImplementedError()

    @abstractmethod
    def exists(self, path: str) -> bool:
        raise NotImplementedError()


class LocalDirectoryStorage(AbstractFileStorage):
    def error(self, e):
        raise e

    def get_files(self, directory: str) -> Generator[str, None, None]:
        directory = os.path.abspath(directory)
        if not self.exists(directory):
            raise Exception(f"Directory {directory} does not exists.")
        for dirpath, dirs, files in os.walk(directory, onerror=self.error):
            for file in files:
                yield os.path.join(dirpath, file)

    def exists(self, path: str) -> bool:
        return os.path.exists(path)


class S3Storage(AbstractFileStorage):
    """
    Minimalistic example of S3 storage.

    Use following environment variables to set up S3 access:
    * AWS_ACCESS_KEY_ID
    * AWS_SECRET_ACCESS_KEY
    * AWS_DEFAULT_REGION

    Tested with localstack S3 implementation only,
    which accepts *any* AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY.
    """

    def __init__(
        self,
        s3_resource: boto3.resources.base.ServiceResource,
        bucket_name: str,
    ):
        self.s3_resource = s3_resource
        self.s3_bucket = s3_resource.Bucket(bucket_name)

    def get_files(self, directory: str) -> Generator[str, None, None]:
        for file in self.s3_bucket.objects.filter(Prefix=directory):
            yield f"s3://{self.s3_bucket.name}/{file.key}"

    def exists(self, path: str) -> bool:
        try:
            self.s3_resource.Object(
                self.s3_bucket.name, self.full_path_to_key_name(path)
            ).load()
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
        return True

    def full_path_to_key_name(self, path: str) -> str:
        """
        Convert s3://mybucket/full/path.txt to just full/path.txt
        """
        return path.split("//")[1].split("/", 1)[1]
