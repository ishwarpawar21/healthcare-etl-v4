from kedro.framework.hooks import hook_impl
from pyspark import SparkConf
from pyspark.sql import SparkSession
from typing import Dict
from kedro.pipeline import Pipeline, pipeline

from healthcare_etl_v4.pipelines.data_processing import pipeline as dp_pipeline
from healthcare_etl_v4.pipelines.data_to_snowflake import pipeline as dts_pipeline


class SparkHooks:
    @hook_impl
    def after_context_created(self, context) -> None:
        """Initialises a SparkSession using the config
        defined in project's conf folder.
        """

        # Load the spark configuration in spark.yaml using the config loader
        parameters = context.config_loader["spark"]
        spark_conf = SparkConf().setAll(parameters.items())

        # Initialise the spark session
        spark_session_conf = (
            SparkSession.builder.appName(context.project_path.name)
            .enableHiveSupport()
            .config(conf=spark_conf)
        )
        _spark_session = spark_session_conf.getOrCreate()
        # _spark_session.sparkContext.setLogLevel("WARN")


from typing import Dict
from kedro.framework.hooks import hook_impl
from kedro.pipeline import Pipeline
from healthcare_etl_v4.pipelines.data_processing import pipeline as dp_pipeline
from healthcare_etl_v4.pipelines.data_to_snowflake import pipeline as dts_pipeline

class ProjectHooks:
    @hook_impl
    def register_pipelines(self) -> Dict[str, Pipeline]:
        return {
            "__default__": dp_pipeline.create_pipeline() + dts_pipeline.create_pipeline(),
            "data_processing": dp_pipeline.create_pipeline(),
            "data_to_snowflake": dts_pipeline.create_pipeline(),
        }