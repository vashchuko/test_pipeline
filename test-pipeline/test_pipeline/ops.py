from dagster import op, OpExecutionContext
from .env_config import EnvConfig
from .create_task_cvat_config import CreateTaskCvatOpConfig
from .load_annotations_config import LoadAnnotationsOpConfig
from .minio_resource import MinioResource
from .cvat_resource import CvatResource

import subprocess
import os
import pathlib
import json


@op
def create_dataset_manifest(context: OpExecutionContext, 
                            config: EnvConfig):
    """create_dataset_manifest - creates manifest for dataset using cvat docker instance

    Args:
        context (OpExecutionContext): dagster execution context
        config (EnvConfig): Op config

    Returns:
        None
    """    

    context.log.info(f'Selected dataset root location: {config.dataset_manifest_location}')

    if os.path.isfile(os.path.join(config.dataset_manifest_location, 'manifest.jsonl')):
        os.remove(os.path.join(config.dataset_manifest_location, 'manifest.jsonl'))

    if os.path.isfile(os.path.join(config.dataset_manifest_location, 'index.json')):   
        os.remove(os.path.join(config.dataset_manifest_location, 'index.json'))

    command = f'docker run -it --rm -v "{config.dataset_manifest_location}":"/local" --entrypoint python3 cvat/server utils/dataset_manifest/create.py --output-dir /local /local/images'
    context.log.info(command)
    try:
        manifest_generation = subprocess.run(command, check=True)
        context.log.info(manifest_generation)
        manifest_generation.check_returncode()

        if not os.path.isfile(os.path.join(config.dataset_manifest_location, 'manifest.jsonl')) or not os.path.isfile(os.path.join(config.dataset_manifest_location, 'index.json')):
            context.log.error('Manifest file was not created!')
            raise FileNotFoundError()
        return manifest_generation
    except subprocess.CalledProcessError as e:
        context.log.error(f'Error happened: {e.output}')
        raise


@op
def upload_dataset(context: OpExecutionContext, 
                   config: EnvConfig, 
                   minio_resource: MinioResource, 
                   arg) -> int:
    """upload_dataset - uploads dataset to storage resource

    Args:
        context (OpExecutionContext): dagster execution context
        config (EnvConfig): Op config,
        minio_resource (MinioResource): storage resource,
        arg (any): input parameters from previous op

    Returns:
        None
    """    
    
    context.log.info(arg)
    if not os.path.isfile(os.path.join(config.dataset_manifest_location, 'manifest.jsonl')) or not os.path.isfile(os.path.join(config.dataset_manifest_location, 'index.json')):
        context.log.error('Manifest doesn\'t exist!')
        raise FileNotFoundError()
    
    ## Upload manifest files first
    
    minio_resource.upload_file(context,
                               config.bucket_name, 
                               'manifest.jsonl',
                               os.path.join(config.dataset_manifest_location, 'manifest.jsonl'),
                               overwrite_existing=True)
    
    
    minio_resource.upload_file(context,
                               config.bucket_name, 
                               'index.json',
                               os.path.join(config.dataset_manifest_location, 'index.json'),
                               overwrite_existing=True)
    
    files = [f for f in pathlib.Path(config.dataset_images).glob("*.jpg")]

    for file in files:
        context.log.info(f'Uploading file from location: {file}')

        try:
            minio_resource.upload_file(context,
                               config.bucket_name, 
                               file.name,
                               file)
        except Exception as e:
            context.log.warning(e)
    return 0


@op
def create_task_cvat(context: OpExecutionContext, 
                     config: CreateTaskCvatOpConfig, 
                     cvat_resource: CvatResource, 
                     arg):
    """create_task_cvat - creates task in CVAT

    Args:
        context (OpExecutionContext): dagster execution context
        config (CreateTaskCvatOpConfig): Op config,
        cvat_resource (CvatResource): CVAT resource,

    Returns:
        None
    """    
    
    task_config = json.loads(config.task_config)
    cloud_storage = cvat_resource.create_cloud_storage(context=context,
                                       resource_name=config.resource_name,
                                       display_name=config.resource_display_name,
                                       key=config.s3_key,
                                       secret_key=config.s3_secret_key,
                                       s3_host=config.s3_host,
                                       manifests=['manifest.jsonl'])
    context.log.info(f'cvat_resource.create_cloud_storage() result: {cloud_storage}')
    
    cvat_resource.create_task_with_data(context=context, 
                                        task_name=task_config['name'],
                                        labels=task_config['labels'],
                                        cloud_storage_id=cloud_storage.id)


@op
def load_annotations(context: OpExecutionContext, 
                     config: LoadAnnotationsOpConfig, 
                     cvat_resource: CvatResource):
    """load_annotations - loads annotations from task

    Args:
        context (OpExecutionContext): dagster execution context
        config (CreateTaskCvatOpConfig): Op config,
        cvat_resource (CvatResource): CVAT resource,

    Returns:
        None
    """    
    task_config = json.loads(config.task_config)
    task = cvat_resource.get_task(context=context,
                                  task_name=task_config['name'])
    context.log.info(f'cvat_resource.get_task() result: {task}')

    task_jobs = cvat_resource.fetch_task_jobs(context=context,
                                             task_id=task.id)
    context.log.info(f'cvat_resource.fetch_task_jobs() result: {task_jobs}')
    
    for job in task_jobs:
        images_annotations = cvat_resource.get_job_annotations(context=context,
                                                              job_id=job.id)
    
        for image in images_annotations:
            context.log.info(image)

            with open(os.path.join(config.data_path, f'{image["name"].split(".")[0]}.json'), 'wb') as f:
                f.write(str(image).encode())