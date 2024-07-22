
# src/healthcare_etl_v4/pipelines/data_to_snowflake/pipeline.py

from kedro.pipeline import Pipeline, node
from .nodes import load_to_snowflake
import warnings
warnings.filterwarnings('ignore')

def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            node(
                func=load_to_snowflake,
                inputs=["healthcare_master", "params:snowflake_config"],
                outputs=None,
                name="load_to_snowflake_node",
            ),
        ]
    )