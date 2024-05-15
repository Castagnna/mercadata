from tools.caos.aws import AWSObjectStorageClient
# from tools.caos.gcp import GCPObjectStorageClient
# from tools.caos.azure import AzureObjectStorageClient


class CAOS_client:
    """Cloud-Agnostic Object Storage client"""
    def __new__(cls, provider: str) -> object:
        match provider:
            case "aws":
                return AWSObjectStorageClient()
            # TODO: Implement GCP Object Storage client
            # case "gcp":
            #     return GCPObjectStorageClient()
            # TODO: Implement Azure Object Storage client
            # case "azure":
            #     return AzureObjectStorageClient()
            case _:
                raise ValueError(f"Invalid provider: {provider}")
