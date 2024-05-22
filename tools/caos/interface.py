from abc import ABC, abstractmethod


class IObjectStorageClient(ABC):
    """Defines an interface class for Object Storage."""
    def __init__(
        self,
        provider: str,
    ) -> None:
        self.__provider = provider

    @property
    def provider(self) -> str:
        return self.__provider

    @abstractmethod
    def ls(self, *args, **kwargs):
        """List all files in a directory."""
        pass

    @abstractmethod
    def exists(self, *args, **kwargs):
        """Check if a file or directory exists."""
        pass

    @abstractmethod
    def open(self, *args, **kwargs):
        """Open a file."""
        pass

    @abstractmethod
    def sparkify(self, path: str, *args, **kwargs) -> str:
        """Convert a path to a Spark-compatible path."""
        pass
