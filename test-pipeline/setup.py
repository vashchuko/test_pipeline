from setuptools import find_packages, setup

setup(
    name="test_pipeline",
    packages=find_packages(exclude=["test_pipeline_tests"]),
    install_requires=[
        "dagster",
        "minio",
        "cvat-sdk",
        "xmltodict"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
