from dagster import Config


class LoadAnnotationsOpConfig(Config):
    task_config: str
    data_path: str