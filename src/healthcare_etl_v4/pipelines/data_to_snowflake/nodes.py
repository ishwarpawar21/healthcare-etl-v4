import pandas as pd
from pyspark.sql import DataFrame
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import warnings
warnings.filterwarnings('ignore')

def load_to_snowflake(healthcare_master: DataFrame, snowflake_config: dict) -> None:
    # Convert Spark DataFrame to Pandas DataFrame
    healthcare_master_pd = healthcare_master.toPandas()
    # Handle large text fields and ensure they are within reasonable limits
    max_length = 16777216  # Example max length for strings
    for col in ['address', 'description', 'source_description']:
        if col in healthcare_master_pd.columns:
            healthcare_master_pd[col] = healthcare_master_pd[col].astype(str).str[:max_length]

    # Convert columns to appropriate data types
    healthcare_master_pd['patient_id'] = healthcare_master_pd['patient_id'].astype('string')
    healthcare_master_pd['birth_date'] = healthcare_master_pd['birth_date'].apply(lambda x: pd.to_datetime(x, errors='coerce'))
    # healthcare_master_pd['birth_date'] = healthcare_master_pd['birth_date'].astype('datetime64[ns]')
    healthcare_master_pd['social_security_number'] = healthcare_master_pd['social_security_number'].astype('string')
    healthcare_master_pd['driversLicense'] = healthcare_master_pd['driversLicense'].astype('string')
    healthcare_master_pd['passport'] = healthcare_master_pd['passport'].astype('string')
    healthcare_master_pd['prefix'] = healthcare_master_pd['prefix'].astype('string')
    healthcare_master_pd['first_name'] = healthcare_master_pd['first_name'].astype('string')
    # healthcare_master['last_name'] = healthcare_master_pd['last_name'].astype('string')
    healthcare_master_pd['suffix'] = healthcare_master_pd['suffix'].astype('string')
    healthcare_master_pd['maiden_name'] = healthcare_master_pd['maiden_name'].astype('string')
    healthcare_master_pd['marital_status'] = healthcare_master_pd['marital_status'].astype('string')
    healthcare_master_pd['race'] = healthcare_master_pd['race'].astype('string')
    healthcare_master_pd['ethnicity'] = healthcare_master_pd['ethnicity'].astype('string')
    healthcare_master_pd['birthPlace'] = healthcare_master_pd['birthPlace'].astype('string')
    healthcare_master_pd['address'] = healthcare_master_pd['address'].astype('string')
    healthcare_master_pd['city'] = healthcare_master_pd['city'].astype('string')
    healthcare_master_pd['state'] = healthcare_master_pd['state'].astype('string')
    healthcare_master_pd['county'] = healthcare_master_pd['county'].astype('string')
    healthcare_master_pd['Fips'] = healthcare_master_pd['Fips'].astype('int64')
    healthcare_master_pd['zip_code'] = healthcare_master_pd['zip_code'].astype('string')
    healthcare_master_pd['latitude'] = healthcare_master_pd['latitude'].astype('float64')
    healthcare_master_pd['longitude'] = healthcare_master_pd['longitude'].astype('float64')
    healthcare_master_pd['healthcare_expenses'] = healthcare_master_pd['healthcare_expenses'].astype('float64')
    healthcare_master_pd['healthcare_coverage'] = healthcare_master_pd['healthcare_coverage'].astype('float64')
    healthcare_master_pd['income'] = healthcare_master_pd['income'].astype('int64')
    healthcare_master_pd['age'] = healthcare_master_pd['age'].astype('float64')
    healthcare_master_pd['GENDER'] = healthcare_master_pd['GENDER'].astype('string')
    healthcare_master_pd['pathology'] = healthcare_master_pd['pathology'].astype('string')
    healthcare_master_pd['Rash_symptoms'] = healthcare_master_pd['Rash_symptoms'].astype('float64')
    healthcare_master_pd['joint_pain_symptoms'] = healthcare_master_pd['joint_pain_symptoms'].astype('float64')
    healthcare_master_pd['fatigue_symptoms'] = healthcare_master_pd['fatigue_symptoms'].astype('float64')
    healthcare_master_pd['fever_symptoms'] = healthcare_master_pd['fever_symptoms'].astype('float64')
    healthcare_master_pd['recorded_date'] = healthcare_master_pd['recorded_date'].astype('datetime64[ns]')
    healthcare_master_pd['resolved_date'] = healthcare_master_pd['resolved_date'].astype('datetime64[ns]')
    healthcare_master_pd['encounter_id_fk'] = healthcare_master_pd['encounter_id_fk'].astype('string')
    healthcare_master_pd['source_code'] = healthcare_master_pd['source_code'].astype('string')
    healthcare_master_pd['source_description'] = healthcare_master_pd['source_description'].astype('string')
    healthcare_master_pd['prescribing_date'] = healthcare_master_pd['prescribing_date'].astype('datetime64[ns]')
    healthcare_master_pd['dispensing_date'] = healthcare_master_pd['dispensing_date'].astype('datetime64[ns]')
    healthcare_master_pd['payer_id'] = healthcare_master_pd['payer_id'].astype('string')
    healthcare_master_pd['encounter_id_fkm'] = healthcare_master_pd['encounter_id_fkm'].astype('string')
    healthcare_master_pd['medication_code'] = healthcare_master_pd['medication_code'].astype('string')
    healthcare_master_pd['medication_description'] = healthcare_master_pd['medication_description'].astype('string')
    healthcare_master_pd['base_cost'] = healthcare_master_pd['base_cost'].astype('float64')
    healthcare_master_pd['payer_coverage'] = healthcare_master_pd['payer_coverage'].astype('float64')
    # healthcare_master_pd['dispenses'] = healthcare_master_pd['dispenses'].astype('float64f')
    healthcare_master_pd['total_cost'] = healthcare_master_pd['total_cost'].astype('float64')
    healthcare_master_pd['reason_code'] = healthcare_master_pd['reason_code'].astype('float64')  # DecimalType mapped to float
    healthcare_master_pd['reason_description'] = healthcare_master_pd['reason_description'].astype('string')
    healthcare_master_pd['encounter_id'] = healthcare_master_pd['encounter_id'].astype('string')
    # # healthcare_master_pd['encounter_start_date'] = pd.to_datetime(healthcare_master_pd['encounter_start_date'], errors='coerce')
    # # healthcare_master_pd['encounter_end_date'] = pd.to_datetime(healthcare_master_pd['encounter_end_date'], errors='coerce')
    healthcare_master_pd['organization'] = healthcare_master_pd['organization'].astype('string')
    healthcare_master_pd['provider'] = healthcare_master_pd['provider'].astype('string')
    healthcare_master_pd['payer'] = healthcare_master_pd['payer'].astype('string')
    healthcare_master_pd['encounter_type'] = healthcare_master_pd['encounter_type'].astype('string')
    healthcare_master_pd['code'] = healthcare_master_pd['code'].astype('string')
    healthcare_master_pd['description'] = healthcare_master_pd['description'].astype('string')
    healthcare_master_pd['paid_amount'] = healthcare_master_pd['paid_amount'].astype('float64')
    healthcare_master_pd['allowed_amount'] = healthcare_master_pd['allowed_amount'].astype('float64')
    healthcare_master_pd['charge_amount'] = healthcare_master_pd['charge_amount'].astype('float64')
    healthcare_master_pd['admit_source_code'] = healthcare_master_pd['admit_source_code'].astype('string')
    healthcare_master_pd['admit_source_description'] = healthcare_master_pd['admit_source_description'].astype('string')
        
    # print(healthcare_master.info())
    # Create Snowflake connection
    conn = snowflake.connector.connect(
        user=snowflake_config['sfUser'],
        password=snowflake_config['sfPassword'],
        account=snowflake_config['sfURL'],
        warehouse=snowflake_config['sfWarehouse'],
        database=snowflake_config['sfDatabase'],
        schema=snowflake_config['sfSchema'],
        role=snowflake_config.get('sfRole', '')
    )
    # Write data to Snowflake using Snowflake connector's write_pandas function
    try:
        with conn.cursor() as cursor:
            # Ensure schema exists
            cursor.execute(f"USE SCHEMA {snowflake_config['sfSchema']}")
            
            # Write data using the Snowflake connector's write_pandas function
            write_pandas(conn, healthcare_master_pd, "HEALTHCARE_MASTER", quote_identifiers=False, overwrite=True)
        
        print("Successfully loaded data into Snowflake.")
    except Exception as e:
        print(f"Failed to load data to Snowflake: {e}")
        raise
    finally:
        conn.close()