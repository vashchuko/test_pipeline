from dagster import Config


class CreateTaskCvatOpConfig(Config):
    s3_key: str
    s3_secret_key: str

    s3_host: str
    resource_name: str
    resource_display_name: str

    task_config: str