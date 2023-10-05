from dagster import ConfigurableResource, OpExecutionContext
from dagster._core.execution.context.init import InitResourceContext
from minio import Minio
from minio.api import ObjectWriteResult

from pydantic import PrivateAttr


class MinioResource(ConfigurableResource):
    """
    A class used to represent a MINIO resource that is used to store dataset

    ...

    Attributes
    ----------
    access_key : str
        access key to the resource
    secret_key : str
        secret key to the resource
    api_host : str
        host address for minio instance
    _minio_client : Configuration
        client for minio instance
    _attemps : Configuration
        number of attempts for upload method
    """
    access_key: str
    secret_key: str

    api_host: str

    _minio_client: Minio = PrivateAttr()
    _attemps: int = PrivateAttr()
    

    def setup_for_execution(self, context: InitResourceContext) -> None:
        self._minio_client = Minio(self.api_host, 
                                access_key=self.access_key, 
                                secret_key=self.secret_key, 
                                secure=False)
        
        self._attemps = 5
    
    
    def create_bucket(self, bucket: str) -> None:
        """create_bucket - creates bucket on minio server

        Args:
            bucket (str): bucket name,

        Returns:
            None
        """

        if self._minio_client.bucket_exists(bucket):
            return None
        
        creation_result = self._minio_client.make_bucket(bucket_name=bucket)
        return creation_result


    def upload_file(self, 
                    context: OpExecutionContext, 
                    bucket: str, 
                    file_name: str, 
                    file_path: str, 
                    overwrite_existing: bool = False) -> ObjectWriteResult:
        """upload_file - uploads file to the specified bucket

        Args:
            context (OpExecutionContext): dagster execution context
            bucket (str): bucket name,
            file_name (str): file name,
            file_path (str): path for file,
            overwrite_existing (bool): specify if file should be overwritten

        Returns:
            write_result (ObjectWriteResult): info about write result
        """

        if not self._minio_client.bucket_exists(bucket):
            raise Exception('Bucket doesn\'t exist')
        
        if overwrite_existing == False and self.is_file_exist(bucket, file_name):
            return None
        
        for i in range(1, self._attemps):
            try:
                upload_result = self._minio_client.fput_object(bucket_name=bucket,
                                            object_name=file_name,
                                            file_path=file_path)
                return upload_result
                
            except Exception as e:
                context.log.info(f'Exception {e} on attempt {i} during loading file {file_path}')
        
        raise Exception(f'File {file_name} not uploaded')
    

    def is_file_exist(self, bucket: str, file_name: str) -> bool:
        """is_file_exist - checks if file exists on resource

        Args:
            context (OpExecutionContext): dagster execution context
            bucket (str): bucket name,
            file_name (str): file name,

        Returns:
            write_result (ObjectWriteResult): info about write result
        """
        
        try:
            self._minio_client.stat_object(bucket,
                                          object_name=file_name)
            return True
        except:
            return False