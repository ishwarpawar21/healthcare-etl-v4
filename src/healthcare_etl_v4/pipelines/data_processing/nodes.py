# src/pipelines/data_processing/nodes.py
import warnings
warnings.filterwarnings('ignore')

import pandas as pd
from pyspark.sql import SparkSession
import numpy as np
from datetime import datetime
import pandas as pd
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, Row, StringType, StructField, StructType
import great_expectations as ge
from great_expectations.data_context import DataContext


def join_datasets_spark(transformed_symptoms: pd.DataFrame,
                        joined_patients: pd.DataFrame,
                        cleaned_conditions: pd.DataFrame,
                        cleaned_medications: pd.DataFrame,
                        cleaned_encounters: str) -> DataFrame:
    
    
    # # Read cleaned_encounters Spark Dataset
    # cleaned_encounters_spark = spark.read.parquet(cleaned_encounters)
    
    # Perform joins using Spark DataFrames
    master_spark_df = joined_patients.join(transformed_symptoms, on='patient_id', how='left')
    master_spark_df = master_spark_df.join(cleaned_conditions, on='patient_id', how='left')
    master_spark_df = master_spark_df.join(cleaned_medications, on='patient_id' , how='left')
    master_spark_df = master_spark_df.join(cleaned_encounters, on='patient_id', how='left')
    
    # Convert Spark DataFrame to Pandas DataFrame
    # master_spark_df = master_spark_df.toPandas()
    return master_spark_df



def extract_and_transform_symptoms(cleaned_symptoms: pd.DataFrame) -> pd.DataFrame:
# Function to split the SYMPTOMS column into four separate columns
    def extract_symptoms(symptoms_str):
        symptoms = symptoms_str.split(';')
        symptoms_dict = {}
        for symptom in symptoms:
            symptom_name, symptom_value = symptom.split(':')
            symptoms_dict[symptom_name + "_symptoms"] = int(symptom_value)
        return symptoms_dict

    # Apply the function to extract symptoms
    symptoms_extracted = cleaned_symptoms['symptoms'].apply(extract_symptoms)
    symptoms_df = pd.json_normalize(symptoms_extracted).fillna(0)
    
    symptoms_df.rename(columns={
        'Joint Pain_symptoms': 'joint_pain_symptoms',
        'Fatigue_symptoms': 'fatigue_symptoms',
        'Fever_symptoms':'fever_symptoms'
    }, inplace=True)

    # Join the extracted symptoms with the original DataFrame (excluding the original SYMPTOMS column)
    cleaned_symptoms = cleaned_symptoms.join(symptoms_df)
    cleaned_symptoms = cleaned_symptoms.drop(columns=['symptoms'])

    return cleaned_symptoms


# join_patients_and_gender

def merge_data(pd_database_1: pd.DataFrame, pd_database_2: pd.DataFrame, primary_key) -> DataFrame:
    # # Initialize Spark session
    # spark = SparkSession.builder \
    # .appName("Join and Save DataFrames") \
    # .getOrCreate()

    # cleaned_patients_spark = spark.createDataFrame(cleaned_patients)
    # cleaned_patients_gender_spark = spark.createDataFrame(cleaned_patients_gender)
    # joined_df = cleaned_patients_spark.join(cleaned_patients_gender_spark, on='patient_id', how='inner')
    joined_df = pd.merge(pd_database_1, pd_database_2, on= primary_key, how='inner')

    return joined_df


def clean_patients_gender_data(patient_gender_df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess patients data:
    - Rename columns to a consistent format
    - Standardize the gender field
    - Handle missing values
    
    Args:
    patient_gender_df: Raw dataframe containing patients data
    
    Returns:
    Preprocessed dataframe
    """
    # Example preprocessing steps
    patient_gender_df.rename(columns={
        'Id': 'patient_id',
        'Gender': 'gender'
    }, inplace=True)

    #patient_gender_df['gender'] = patient_gender_df['gender'].str.title()
    #patient_gender_df['gender'] = patient_gender_df['gender'].replace({'M': 'Male', 'F': 'Female', 'U': 'Unknown'})

    patient_gender_df.fillna({'gender': 'Unknown'}, inplace=True)

    return patient_gender_df

def clean_patients_data(patients_df: pd.DataFrame, context_path: str) -> pd.DataFrame:
    """
    Preprocess patients data:
    - Rename columns to match Tuva Data Model Input Layer
    - Convert 'birth_date' to datetime format and calculate age
    - Remove records with missing Birth Date
    - Remove negative values from Income
    - Standardize prefix and suffix
    - Handle age outliers
    - Handle healthcare_expenses outliers using IQR method
    
    Args:
    patients_df: Raw dataframe containing patients data
    
    Returns:
    Preprocessed dataframe
    """
    # Define the updated column mapping
    column_mapping = {
    'PATIENT_ID': 'patient_id',
    'FIRST': 'first_name',
    'LAST': 'last_name',
    'GENDER': 'sex',
    'RACE': 'race',
    'BIRTHDATE': 'birth_date',
    'DEATHDATE': 'death_date',
    'SSN': 'social_security_number',
    'ADDRESS': 'address',
    'CITY': 'city',
    'STATE': 'state',
    'ZIP': 'zip_code',
    'COUNTY': 'county',
    'LAT': 'latitude',
    'LON': 'longitude',
    'INCOME':'income',
    'HEALTHCARE_EXPENSES': 'healthcare_expenses',
    'HEALTHCARE_COVERAGE': 'healthcare_coverage',
    'PASSPORT': 'passport',
    'SUFFIX': 'suffix',
    'BIRTHPLACE': 'birthPlace',
    'ETHNICITY': 'ethnicity',
    'MARITAL': 'marital_status',
    'DRIVERS': 'driversLicense',
    'PREFIX': 'prefix',
    'MAIDEN': 'maiden_name',
    'FIPS': 'Fips',
    }
    
    # Rename columns
    patients_df.rename(columns=column_mapping, inplace=True)
    
    # Drop Duplicate Records
    patients_df.drop_duplicates(inplace=True)

    # Convert DATE columns to datetime format
    patients_df['birth_date'] = pd.to_datetime(patients_df['birth_date'], errors='coerce')
    patients_df['death_date'] = pd.to_datetime(patients_df['death_date'], errors='coerce')

    # Remove columns with all null values (sex data from other data source and death_date removing null death_date coloumn)
    patients_df.drop(columns=['death_date', 'sex'], inplace=True)

    # Validate and clean the data


    # Ensure SSN format is valid
    patients_df = patients_df[patients_df['social_security_number'].str.match(r'\d{3}-\d{2}-\d{4}')]

    # Ensure ZIP code is a valid 5-digit number
    patients_df = patients_df[patients_df['zip_code'].astype(str).str.match(r'^\d{5}$')]

    # Ensure LAT and LON are valid geographical coordinates
    patients_df = patients_df[(patients_df['latitude'].between(-90, 90)) & (patients_df['longitude'].between(-180, 180))]

    # # Remove invalid LAT and LON
    # patients_df = patients_df.dropna(subset=['latitude', 'longitude'])
    
    # # feature enginnering coloumn age.
    patients_df['age'] = (datetime.now() - patients_df['birth_date']).dt.days // 365

    # Format data
    patients_df['prefix'] = patients_df['prefix'].str.strip().str.title()
    patients_df['suffix'] = patients_df['suffix'].str.strip().str.upper()

    # Remove invalid birth dates
    patients_df = patients_df[patients_df['birth_date'] != '9999-99-99']
    mean_age = patients_df['age'].mean()
    patients_df['age'] = np.where((patients_df['age'] < 0) | (patients_df['age'] > 120), mean_age, patients_df['age'])

    Q1 = patients_df['healthcare_expenses'].quantile(0.25)
    Q3 = patients_df['healthcare_expenses'].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    
    patients_df['healthcare_expenses'] = np.where((patients_df['healthcare_expenses'] < lower_bound) | (patients_df['healthcare_expenses'] > upper_bound), np.nan, patients_df['healthcare_expenses'])

   
        # Initialize GE context
    context = ge.get_context()

    # Convert to a Great Expectations DataFrame
    ge_df = ge.from_pandas(patients_df)

    # Define the expectation suite name
    expectation_suite_name = "cleaned_patients_asset_name.warning"

    # Load the expectation suite or create a new one
    try:
        suite = context.get_expectation_suite(expectation_suite_name=expectation_suite_name)
        print(f'Loaded ExpectationSuite "{suite.expectation_suite_name}" containing {len(suite.expectations)} expectations.')
    except ge.exceptions.DataContextError:
        print(f'Expectation suite "{expectation_suite_name}" does not exist. Creating a new one.')
        suite = context.add_expectation_suite(expectation_suite_name=expectation_suite_name)


    # Perform the validation
    validation_result = context.run_validation_operator(
        "action_list_operator",
        assets_to_validate=[ge_df],
        run_id="patients_validation")

    # # Validate the data
    # results = context.run_validation_operator(
    #     "action_list_operator",
    #     assets_to_validate=[ge_df],
    #     run_id="patients_data_validation"
    # )
    
    # Check validation results
    if not validation_result["success"]:
        raise ValueError("Data validation failed")
    else:
        return patients_df
    
    
    
    return patients_df

def clean_medications_data(medications_df: pd.DataFrame) -> pd.DataFrame:

    # Define column mapping based on the Tuva Project
    column_mapping = {
            'START': 'prescribing_date',
            'STOP': 'dispensing_date',
            'PATIENT': 'patient_id',
            'PAYER': 'payer_id',
            'ENCOUNTER': 'encounter_id_fkm',
            'CODE': 'medication_code',
            'DESCRIPTION': 'medication_description',
            'BASE_COST': 'base_cost',
            'PAYER_COVERAGE': 'payer_coverage',
            'DISPENSES': 'dispenses',
            'TOTALCOST': 'total_cost',
            'REASONCODE': 'reason_code',
            'REASONDESCRIPTION': 'reason_description'
    }

    # Rename columns
    medications_df.rename(columns=column_mapping, inplace=True)

    # Drop Duplicate Records
    medications_df.drop_duplicates(inplace=True)

    # Convert DATE columns to datetime format
    medications_df['dispensing_date'] = pd.to_datetime(medications_df['dispensing_date'], errors='coerce')
    medications_df['prescribing_date'] = pd.to_datetime(medications_df['prescribing_date'], errors='coerce')

    # Track the initial record count
    total_records = len(medications_df)

    # Remove invalid invalid date, base_cost and total_cost values
    medications_df = medications_df[medications_df['prescribing_date'] <= medications_df['dispensing_date']]
    medications_df = medications_df[medications_df['total_cost'] >= 0]
    medications_df = medications_df[medications_df['total_cost'] >= 0]

    # Check for duplicate records
    duplicate_count = medications_df.duplicated().sum()

    # Remove outliers
    Q1_payer_coverage = medications_df['payer_coverage'].quantile(0.25)
    Q3_payer_coverage = medications_df['payer_coverage'].quantile(0.75)
    IQR_payer_coverage = Q3_payer_coverage - Q1_payer_coverage
    lower_bound_payer_coverage = Q1_payer_coverage - 1.5 * IQR_payer_coverage
    upper_bound_payer_coverage = Q3_payer_coverage + 1.5 * IQR_payer_coverage
    medications_df = medications_df[(medications_df['payer_coverage'] >= lower_bound_payer_coverage) & (medications_df['payer_coverage'] <= upper_bound_payer_coverage)]

    Q1_dispenses = medications_df['dispenses'].quantile(0.25)
    Q3_dispenses = medications_df['dispenses'].quantile(0.75)
    IQR_dispenses = Q3_dispenses - Q1_dispenses
    lower_bound_dispenses = Q1_dispenses - 1.5 * IQR_dispenses
    upper_bound_dispenses = Q3_dispenses + 1.5 * IQR_dispenses
    medications_df = medications_df[(medications_df['dispenses'] >= lower_bound_dispenses) & (medications_df['dispenses'] <= upper_bound_dispenses)]

    Q1_base_cost = medications_df['base_cost'].quantile(0.25)
    Q3_base_cost = medications_df['base_cost'].quantile(0.75)
    IQR_base_cost = Q3_base_cost - Q1_base_cost
    lower_bound_base_cost = Q1_base_cost - 1.5 * IQR_base_cost
    upper_bound_base_cost = Q3_base_cost + 1.5 * IQR_base_cost
    medications_df = medications_df[(medications_df['base_cost'] >= lower_bound_base_cost) & (medications_df['base_cost'] <= upper_bound_base_cost)]

    # Standardize medication description
    medications_df['medication_description'] = medications_df['medication_description'].str.strip().str.title()
   
    return medications_df

def clean_symptoms_data(symptoms_df: pd.DataFrame) -> pd.DataFrame:
    column_mapping = {
        'PATIENT': 'patient_id',
        'GENDER': 'gender',
        'RACE': 'race',
        'ETHNICITY': 'ethnicity',
        'AGE_BEGIN': 'age_begin',
        'AGE_END': 'age_end',
        'PATHOLOGY': 'pathology',
        'NUM_SYMPTOMS': 'num_symptoms',
        'SYMPTOMS': 'symptoms'
    }

    # Rename columns
    symptoms_df.rename(columns=column_mapping, inplace=True)

    # Remove duplicate records
    duplicate_records = symptoms_df.duplicated().sum()
    symptoms_df.drop_duplicates(inplace=True)

    # Remove duplicated/ null Gender, AGE_BEGIN, AGE_END, race, ethnicitycolumns
    symptoms_df.drop(columns=['gender', 'age_begin', 'age_end','ethnicity','race'], inplace=True)

    return symptoms_df

def clean_conditions_data(conditions_df: pd.DataFrame) -> pd.DataFrame:
    # Rename columns based on the provided mapping
    column_mapping = {
        'START': 'recorded_date',
        'STOP': 'resolved_date',
        'PATIENT': 'patient_id',
        'ENCOUNTER': 'encounter_id_fk',
        'CODE': 'source_code',
        'DESCRIPTION': 'source_description'
    }

    conditions_df.rename(columns=column_mapping, inplace=True)

    # Convert DATE columns to datetime format
    conditions_df['recorded_date'] = pd.to_datetime(conditions_df['recorded_date'], errors='coerce')
    conditions_df['resolved_date'] = pd.to_datetime(conditions_df['resolved_date'], errors='coerce')

    # Remove duplicate records
    conditions_df.drop_duplicates(inplace=True)

    # Detect and remove outliers in the 'source_code' column
    Q1 = conditions_df['source_code'].quantile(0.25)
    Q3 = conditions_df['source_code'].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    conditions_df = conditions_df[(conditions_df['source_code'] >= lower_bound) & (conditions_df['source_code'] <= upper_bound)]  

    return conditions_df


def clean_encounters_data(encounters_df: DataFrame) -> DataFrame:
    # Rename columns based on definitions
    encounters_df = encounters_df.withColumnRenamed("Id","encounter_id") \
                                      .withColumnRenamed("PATIENT", "patient_id") \
                                      .withColumnRenamed("ENCOUNTERCLASS", "encounter_type") \
                                      .withColumnRenamed("START", "encounter_start_date") \
                                      .withColumnRenamed("STOP", "encounter_end_date") \
                                      .withColumnRenamed("ORGANIZATION", "organization") \
                                      .withColumnRenamed("PROVIDER", "provider") \
                                      .withColumnRenamed("PAYER", "payer") \
                                      .withColumnRenamed("CODE", "code") \
                                      .withColumnRenamed("DESCRIPTION", "description") \
                                      .withColumnRenamed("BASE_ENCOUNTER_COST", "paid_amount") \
                                      .withColumnRenamed("TOTAL_CLAIM_COST", "allowed_amount") \
                                      .withColumnRenamed("PAYER_COVERAGE", "charge_amount") \
                                      .withColumnRenamed("REASONCODE", "admit_source_code") \
                                      .withColumnRenamed("REASONDESCRIPTION", "admit_source_description")

    # # Convert START and STOP columns to date format
    # encounters_df = encounters_df.withColumn("encounter_start_date", to_timestamp("encounter_start_date")) \
    #     .withColumn("encounter_end_date", to_timestamp("stop"))

    # Convert columns to appropriate data types
    encounters_df = encounters_df.withColumn("paid_amount", col("paid_amount").cast("float")) \
        .withColumn("allowed_amount", col("allowed_amount").cast("float")) \
        .withColumn("charge_amount", col("charge_amount").cast("float"))
    
    # Count and remove duplicate records
    encounters_df_cleaned = encounters_df.dropDuplicates(["encounter_id"])

    # Remove columns with all null values (if any)
    encounters_df_cleaned = encounters_df_cleaned.dropna(how='all')

    # Remove outliers for BASE_ENCOUNTER_COST and TOTAL_CLAIM_COST using IQR
    def remove_outliers(df, column_name):
        quantiles = df.approxQuantile(column_name, [0.25, 0.75], 0.01)
        Q1, Q3 = quantiles[0], quantiles[1]
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        return df.filter((col(column_name) >= lower_bound) & (col(column_name) <= upper_bound))

    encounters_df_cleaned = remove_outliers(encounters_df_cleaned, "paid_amount")
    encounters_df_cleaned = remove_outliers(encounters_df_cleaned, "allowed_amount")
    return encounters_df

