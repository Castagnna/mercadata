import os
from tools.caos.interface import IObjectStorageClient


class OsObjectStorageClient(IObjectStorageClient):
    """Operational System Object Storage client."""

    def __init__(
        self,
        provider: str = "os",
        **kwargs,
    ) -> None:
        self.__provider = provider

    @property
    def provider(self) -> str:
        return self.__provider

    def ls(self, *args, **kwargs):
        return os.listdir(*args, **kwargs)

    def exists(self, *args, **kwargs):
        return os.path.exists(*args, **kwargs)

    def open(self, *args, **kwargs):
        return open(*args, **kwargs)

    @staticmethod
    def sparkify(path: str) -> str:
        return path
