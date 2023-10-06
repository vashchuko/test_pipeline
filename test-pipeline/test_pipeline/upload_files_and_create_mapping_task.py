from dagster import job, EnvVar, RunConfig
from .env_config import EnvConfig
from .create_task_cvat_config import CreateTaskCvatOpConfig
from .ops import create_dataset_manifest, upload_dataset, create_task_cvat


default_config = RunConfig(
    ops={"create_dataset_manifest": EnvConfig(dataset_manifest_location=EnvVar("DATASET_MANIFEST_LOCATION"),
                                              dataset_images=EnvVar("DATASET_IMAGES"),
                                              bucket_name=EnvVar("BUCKET_NAME")),
         "upload_dataset": EnvConfig(dataset_manifest_location=EnvVar("DATASET_MANIFEST_LOCATION"),
                                     dataset_images=EnvVar("DATASET_IMAGES"),
                                     bucket_name=EnvVar("BUCKET_NAME")),
         "create_task_cvat": CreateTaskCvatOpConfig(s3_key=EnvVar('ACCESS_KEY'),
                                                        s3_secret_key=EnvVar('SECRET_KEY'),
                                                        s3_host=EnvVar('S3_API_HOST'),
                                                        resource_name=EnvVar('BUCKET_NAME'),
                                                        resource_display_name=EnvVar('RESOURCE_DISPLAY_NAME'),
                                                        task_config=EnvVar('TASK_CONFIG'))
    },
)

@job(config=default_config)
def upload_files_and_create_mapping_task():
    """upload_files_and_create_mapping_task - job for files upload and mapping task creation

    Args:
        None
    Returns:
        None
    """    
    create_task_cvat(upload_dataset(create_dataset_manifest()))


