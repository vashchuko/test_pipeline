from dagster import EnvVar, Definitions, load_assets_from_modules

from . import assets
from .upload_files_and_create_mapping_task import upload_files_and_create_mapping_task
from .retrieve_annotations import retrieve_annotations
from .minio_resource import MinioResource
from .cvat_resource import CvatResource

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    jobs=[upload_files_and_create_mapping_task, retrieve_annotations],
    resources={
        "minio_resource": MinioResource(access_key=EnvVar('ACCESS_KEY'),
                                        secret_key=EnvVar('SECRET_KEY'),
                                        api_host=EnvVar('MINIO_API_HOST')),
        "cvat_resource": CvatResource(username=EnvVar('CVAT_USERNAME'),
                                      password=EnvVar('CVAT_PASSWORD'),
                                      host=EnvVar('CVAT_HOST'))
    }
)
