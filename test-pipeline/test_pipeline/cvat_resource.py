from http import HTTPStatus
import io
import zipfile
from dagster import ConfigurableResource, OpExecutionContext
from dagster._core.execution.context.init import InitResourceContext
from cvat_sdk.api_client import Configuration, ApiClient, models, exceptions
from cvat_sdk.api_client.models import *
import xmltodict

from pydantic import PrivateAttr


class CvatResource(ConfigurableResource):
    """
    A class used to represent a CVAT resource that is used to connect to CVAT instance

    ...

    Attributes
    ----------
    username : str
        username for CVAT access
    password : str
        password for CVAT access
    host : str
        host address for CVAT instance
    _configuration : Configuration
        configuration object for ApiClient
    """

    username: str
    password: str
    host: str

    _configuration: Configuration = PrivateAttr()


    def setup_for_execution(self, context: InitResourceContext) -> None:
        self._configuration=Configuration(
            host=self.host,
            username=self.username,
            password=self.password
        )


    def create_cloud_storage(self, 
                             context: OpExecutionContext, 
                             resource_name: str, 
                             display_name: str,
                             key: str, 
                             secret_key: str, 
                             s3_host: str, 
                             manifests:[],
                             provider_type: str = "AWS_S3_BUCKET") -> CloudStorageRead:
        """create_cloud_storage - creates cloud storage used to store data for labeling

        Args:
            context (OpExecutionContext): dagster execution context
            resource_name (str): cloud resource name,
            display_name (str): cloud resource display name,
            key (str): key to access a provider, 
            secret_key (str): secret key to access a provider, 
            s3_host (str): provider host, 
            manifests ([]): manifests to access files on provider,
            provider_type (ProviderTypeEnum): provider type 

        Returns:
            cloud_storage (CloudStorageRead): cloud storage object with data
        """

        with ApiClient(self._configuration) as api_client:

            try:
                (data, response) = api_client.cloudstorages_api.list(
                    page=1,
                    page_size=10,
                    provider_type=provider_type,
                    resource=resource_name
                )
                context.log.info(f'CloudstoragesApi.list() result: {data}')
                if data.count > 0:
                    return data.results[0]
            except exceptions.ApiException as e:
                context.log.error("Exception when calling CloudstoragesApi.list(): %s\n" % e)
                raise


            cloud_storage_request = CloudStorageWriteRequest(
                provider_type=ProviderTypeEnum(provider_type),
                resource=resource_name,
                display_name=display_name,
                credentials_type=models.CredentialsTypeEnum("KEY_SECRET_KEY_PAIR"),
                key=key,
                secret_key=secret_key,
                specific_attributes=f"endpoint_url={s3_host}",
                manifests=manifests
            )

            try:
                (data, response) = api_client.cloudstorages_api.create(
                    cloud_storage_request
                )
                context.log.info(f'CloudstoragesApi.create() result: {data}')
                return data
            except exceptions.ApiException as e:
                context.log.error("Exception when calling CloudstoragesApi.create(): %s\n" % e)
                raise


    def get_cloudstorage_content(self, 
                                 context: OpExecutionContext, 
                                 cloud_storage_id: int) -> list[str]:
        """get_cloudstorage_content - gets cloud storage content files

        Args:
            context (OpExecutionContext): dagster execution context
            cloud_storage_id (int): cloud storage id,

        Returns:
            files (List[str]): list of the files stored on cloud storage
        """

        with ApiClient(self._configuration) as api_client:
            try:
                (content_data, response) = api_client.cloudstorages_api.retrieve_content(
                    cloud_storage_id
                )
                #return content_data + ["manifest.jsonl"]
                return content_data
            except exceptions.ApiException as e:
                context.log.error(
                    f"Exception when calling cloudstorages_api.retrieve_content: {e}\n"
                )


    def create_task_with_data(self, 
                              context: OpExecutionContext,
                              task_name: str, 
                              labels: list[tuple[str,str]],
                              cloud_storage_id: int) -> None:
        """create_task_with_data - create task for labeling with data on cloud storage

        Args:
            context (OpExecutionContext): dagster execution context,
            labels (List[tuple[str,str]]): list of labels to assign to task,
            task_name (str): name for a task,
            cloud_storage_id (int): cloud storage id,

        Returns:
            None
        """

        with ApiClient(self._configuration) as api_client:
            created_task = None

            task_write_request = TaskWriteRequest(
                name=task_name,
                labels=[
                    PatchedLabelRequest(
                        id=1,
                        name=label[0],
                        type=label[1]
                    ) for label in labels
                ],
                target_storage=PatchedTaskWriteRequestTargetStorage(args={
                    "location": "local"
                }),
                source_storage=PatchedTaskWriteRequestTargetStorage(args={
                    "location": "cloud_storage",
                    "cloud_storage_id": f"{cloud_storage_id}"
                }),
            )

            try:
                (data, response) = api_client.tasks_api.create(
                    task_write_request,
                )
                created_task = data
                context.log.info(f'TasksApi.create(): {created_task}')
            except exceptions.ApiException as e:
                context.log.error("Exception when calling TasksApi.create(): %s\n" % e)
                raise

            content = self.get_cloudstorage_content(context=context, cloud_storage_id=cloud_storage_id)
            context.log.info(content)

            data_request = models.DataRequest(
                cloud_storage_id=cloud_storage_id,
                image_quality=70,
                server_files=content,
                sorting_method=SortingMethod("lexicographical"),
                use_zip_chunks=True,
                use_cache=True
            )

            try:
                context.log.info(created_task.id)
                (_, response) = api_client.tasks_api.create_data(id=created_task.id, 
                                                                 data_request=data_request)

            except exceptions.ApiException as e:
                context.log.error(f"Exception when calling ProjectsApi.put_task_data: {e}\n")
                raise


    def get_task(self, 
                 context: OpExecutionContext, 
                 task_name: str) -> TaskRead:
        """get_task - get task for labeling by name

        Args:
            context (OpExecutionContext): dagster execution context,
            task_name (str): name for a task,

        Returns:
            task (TaskRead): task object with data
        """

        with ApiClient(self._configuration) as api_client:
            try:
                (data, response) = api_client.tasks_api.list(
                    name=task_name,
                )
                context.log.info(data)
                if data.count > 0:
                    return data.results[0]
            except exceptions.ApiException as e:
                context.log.error("Exception when calling TasksApi.list(): %s\n" % e)


    def fetch_task_jobs(self, 
                        context: OpExecutionContext, 
                        task_id: int) -> list[JobRead]:
        """fetch_task_jobs - get task jobs

        Args:
            context (OpExecutionContext): dagster execution context,
            task_id (str): id for a task,

        Returns:
            jobs (List[JobRead]): List object with jobs data
        """

        with ApiClient(self._configuration) as api_client:
            try:
                (data, response) = api_client.jobs_api.list(task_id=task_id)
                context.log.info(data)
                return data.results
            except exceptions.ApiException as e:
                context.log.error(f"Exception when calling JobsApi.list: {e}\n")


    def get_job_annotations(self, 
                            context: OpExecutionContext, 
                            job_id: int) -> list[dict]:
        """get_job_annotations - get job images annotation

        Args:
            context (OpExecutionContext): dagster execution context,
            job_id (str): id for a job,

        Returns:
            images (List[Dict]): List object with images that contain annotations
        """

        context.log.info(f'Starting annotations loading')
        with ApiClient(self._configuration) as api_client:
            try:
                for i in range(5):
                    (_, response) = api_client.jobs_api.retrieve_annotations(
                        id=job_id,
                        action="download",
                        format="CVAT for images 1.1",
                        _parse_response=False,
                    )
                    if response.status == HTTPStatus.OK:
                        break
                context.log.info(response)

                buffer = io.BytesIO(response.data)
                with zipfile.ZipFile(buffer, "r") as zip_file:
                    xml_content = zip_file.read("annotations.xml").decode("utf-8")
                annotations = xmltodict.parse(xml_content, attr_prefix="")

                return annotations["annotations"]["image"]
            except exceptions.ApiException as e:
                context.log.error(f"Exception when calling JobsApi.retrieve_annotations: {e}\n")
                raise
