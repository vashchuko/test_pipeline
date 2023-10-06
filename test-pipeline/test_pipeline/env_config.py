from dagster import Config


class EnvConfig(Config):
    dataset_manifest_location: str
    dataset_images: str
    bucket_name: str