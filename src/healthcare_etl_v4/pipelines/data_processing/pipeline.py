from kedro.pipeline import Pipeline, node, pipeline
import warnings
warnings.filterwarnings('ignore')

from .nodes import (
    clean_patients_gender_data,
    clean_patients_data,
    clean_medications_data,
    clean_symptoms_data,
    clean_conditions_data,
    clean_encounters_data,
    merge_data,
    extract_and_transform_symptoms,
    join_datasets_spark,
)

def create_pipeline(**kwargs) -> Pipeline:
        
    return pipeline(
        [

            node(
                func=join_datasets_spark,
                inputs={
                    "transformed_symptoms": "transformed_symptoms@spark",
                    "joined_patients": "joined_patients@spark",
                    "cleaned_conditions": "cleaned_conditions@spark",
                    "cleaned_medications": "cleaned_medications@spark",
                    "cleaned_encounters": "cleaned_encounters",
                },
                outputs="healthcare_master",
                name="join_datasets_spark_node"
            ),
            node(
                clean_patients_gender_data,
                inputs="patient_gender",
                outputs="cleaned_patients_gender",
                name="cleaned_patients_gender_data_node",
            ),
            node(
                clean_patients_data,
                inputs=['patients',"params:great_expectations_context_path"],
                outputs='cleaned_patients',
                name='cleaned_patients_node',
             ),
            node(
                    clean_medications_data,
                    inputs="medications",
                    outputs="cleaned_medications@csv",
                    name="clean_medications_data_node",
            ),
            node(
                clean_symptoms_data,
                inputs="symptoms",
                outputs="cleaned_symptoms",
                name="clean_symptoms_data_node",
            ),
            node(
                clean_conditions_data,
                inputs="conditions",
                outputs="cleaned_conditions@csv",
                name="clean_conditions_data_node",
            ),
            node(
                clean_encounters_data,
                inputs="encounters",
                outputs="cleaned_encounters",
                name="clean_encounters_data_node",
            ),
            node(
                merge_data,
                inputs=dict(
                    pd_database_1="cleaned_patients",
                    pd_database_2="cleaned_patients_gender",
                    primary_key = "params:primary_key"
                    #spark="spark_session"
                ),
                outputs="joined_patients@csv",
                name="join_patients_and_gender_node",
            ),
            node(
                extract_and_transform_symptoms,
                inputs="cleaned_symptoms",
                outputs="transformed_symptoms@csv",
                name="extract_and_transform_symptoms_node",
            ),

        ]
    )
