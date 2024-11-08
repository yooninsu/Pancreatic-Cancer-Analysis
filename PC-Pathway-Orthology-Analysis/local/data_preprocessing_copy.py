import pandas as pd
import os
import pickle
import json

def lambda_handler(event, context=None):
    # 로컬 테스트를 위한 임의 버킷 이름
    bucket = event.get('s3_bucket', 'your-local-bucket')

    # Define the dataset type (Pathway or Orthology) from the event
    dataset_type = event.get('dataset_type', 'Pathway')  # Default to Pathway if not provided

    # Determine the file paths based on dataset type
    if dataset_type == 'Pathway':
        function_file_path = 'PC-Pathway/PC Pathway.csv'
        intermediate_folder = 'PC-Pathway/intermediate/'
        preprocessed_data_path = f"{intermediate_folder}preprocessed_data_pathway.pkl"
        missing_columns_path = f"{intermediate_folder}missing_columns_pathway.json"
    else:  # For Orthology
        function_file_path = 'PC-Orthology/PC Orthology.csv'
        intermediate_folder = 'PC-Orthology/intermediate/'
        preprocessed_data_path = f"{intermediate_folder}preprocessed_data_orthology.pkl"
        missing_columns_path = f"{intermediate_folder}missing_columns_orthology.json"
    
    try:
        # 로컬 경로로 파일 읽기 (S3 다운로드 부분 대체)
        print(f"Loading local file from: {function_file_path}")
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

        # Save preprocessed data locally
        data.to_pickle(preprocessed_data_path)
        print(f"Preprocessed data saved at: {preprocessed_data_path}")

        # Save missing columns information to a JSON file
        with open(missing_columns_path, 'w') as f:
            json.dump(missing_columns.tolist(), f)
        print(f"Missing columns saved at: {missing_columns_path}")

        return {
            'statusCode': 200,
            'message': f'{dataset_type} data preprocessing complete.',
            'preprocessed_data_local_path': preprocessed_data_path,
            'missing_columns_local_path': missing_columns_path
        }
    except Exception as e:
        print(f"Error in preprocessing: {e}")
        return {
            'statusCode': 500,
            'error': f"Error in preprocessing: {e}"
        }


# 로컬 테스트
if __name__ == "__main__":
    # event.json 파일에서 event 로드
    with open('event.json') as f:
        event = json.load(f)
    
    # lambda_handler 호출
    result = lambda_handler(event)
    print(json.dumps(result, indent=2))
