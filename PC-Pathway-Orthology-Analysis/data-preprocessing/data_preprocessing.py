import boto3
import pandas as pd
import os
import pickle
import json

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    lambda_client = boto3.client('lambda')  # Initialize Lambda client to invoke the next function
    bucket = event['s3_bucket']
    print(bucket)
    # Define the dataset type (Pathway or Orthology) from the event
    dataset_type = event.get('dataset_type', 'Pathway')  # Default to Pathway if not provided
    print(dataset_type)
    # Determine the file paths based on dataset type
    if dataset_type == 'Pathway':
        function_file_key = 'PC_Pathway/PC Pathway.csv'
        intermediate_folder = 'PC_Pathway/intermediate/'
        preprocessed_data_key = f"{intermediate_folder}preprocessed_data_pathway.pkl"
        missing_columns_key = f"{intermediate_folder}missing_columns_pathway.json"
    else:  # For Orthology
        function_file_key = 'PC_Orthology/PC Orthology.csv'
        intermediate_folder = 'PC_Orthology/intermediate/'
        preprocessed_data_key = f"{intermediate_folder}preprocessed_data_orthology.pkl"
        missing_columns_key = f"{intermediate_folder}missing_columns_orthology.json"
    
    print(function_file_key)
    try:
        # Define local paths for the downloaded files
        function_file_path = f"/tmp/{os.path.basename(function_file_key)}"
        
        # Download the CSV files from S3 to the Lambda /tmp directory
        s3.download_file(bucket, function_file_key, function_file_path)
        
        # Read the CSV files using pandas
        function_data = pd.read_csv(function_file_path)
        
        # Preprocess the data
        X_dat = function_data.iloc[:, 3:]       

                
        # Log the initial shape of the data
        print(f"Initial data shape: {X_dat.shape}")
 
        X = X_dat.apply(pd.to_numeric, errors='coerce')  # Ensure data is numeric
        y = function_data['group_2']  # Ensure 'group_2' exists in your CSV file
        
        # Identify columns with missing values
        missing_columns = X.columns[X.isna().any()]
        
        # Remove columns that contain NaN values
        X_cleaned = X.dropna(axis=1)
        
        
        # Log the shape after cleaning
        print(f"Data shape after cleaning: {X_cleaned.shape}")
        
        # Concatenate target variable and cleaned features
        data = pd.concat([y, X_cleaned], axis=1)
        
        # Save preprocessed data to a pickle file in /tmp
        preprocessed_data_path = f"/tmp/{os.path.basename(preprocessed_data_key)}"
        data.to_pickle(preprocessed_data_path)
        
        # Upload preprocessed data to S3
        s3.upload_file(preprocessed_data_path,bucket, preprocessed_data_key)
        
        # Save missing columns information to a JSON file in /tmp
        missing_columns_path = f"/tmp/{os.path.basename(missing_columns_key)}"
        with open(missing_columns_path, 'w') as f:
            json.dump(missing_columns.tolist(), f)
        
        # Upload missing columns info to S3
        s3.upload_file(missing_columns_path, bucket, missing_columns_key)
        
        return {
            'statusCode': 200,
            'message': f'{dataset_type} data preprocessing complete.',
            'dataset_type' : dataset_type,
            'preprocessed_data_s3_key': preprocessed_data_key,
            'missing_columns_s3_key': missing_columns_key,
            'label_file_s3_key': event.get('label_file_s3_key')  # Pass the label file key from the event

        }
    except Exception as e:
        print(f"Error in preprocessing: {e}")
        return {
            'statusCode': 500,
            'error': f"Error in preprocessing: {e}"
        }

