from os import path as P
from s3fs import S3FileSystem
from tools.caos.interface import IObjectStorageClient


class AWSObjectStorageClient(IObjectStorageClient):
    """AWS Object Storage client."""
    def __init__(
        self,
        provider: str = "aws",
        **kwargs,
    ) -> None:
        super().__init__(provider)
        creds = {k: v for k, v in kwargs.items() if k in ("key", "secret", "token")}
        self.__s3 = S3FileSystem(**creds)

    @property
    def s3(self) -> S3FileSystem:
        return self.__s3

    def ls(self, *args, **kwargs):
        return self.s3.ls(*args, **kwargs)

    def exists(self, *args, **kwargs):
        return self.s3.exists(*args, **kwargs)

    def open(self, *args, **kwargs):
        return self.s3.open(*args, **kwargs)

    @staticmethod
    def sparkify(path: str) -> str:
        return P.join("s3a://", path)
