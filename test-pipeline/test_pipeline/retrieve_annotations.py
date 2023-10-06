from dagster import job, EnvVar, RunConfig
from .load_annotations_config import LoadAnnotationsOpConfig
from .ops import load_annotations


default_config = RunConfig(
        ops={"load_annotations": LoadAnnotationsOpConfig(task_config=EnvVar('TASK_CONFIG'),
                                                         data_path=EnvVar("DATA_PATH"))
    },
)

@job(config=default_config)
def retrieve_annotations():
    """retrieve_annotations - job for retrieving annotations from task

    Args:
        None
    Returns:
        None
    """    
    load_annotations()


if __name__ == "__main__":
    retrieve_annotations.execute_in_process(
        run_config=LoadAnnotationsOpConfig(task_config=EnvVar('TASK_CONFIG'),
                                           data_path=EnvVar("DATA_PATH"))
    )