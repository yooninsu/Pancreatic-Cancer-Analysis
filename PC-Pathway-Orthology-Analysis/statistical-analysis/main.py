import boto3
import pandas as pd
import pickle
import os
import sys

# 현재 파일의 디렉토리를 기준으로 utils 폴더를 Python 경로에 추가
current_dir = os.path.dirname(os.path.abspath(__file__))
utils_path = os.path.join(current_dir, "utils")
sys.path.append(utils_path)
print(utils_path)
print(current_dir)
# Import the helper functions
from utils.compute_statistics import compute_statistics
from utils.logistic_regression_univariate_w_BH import logistic_regression_univariate_w_BH
from utils.lda_analysis import lda_analysis
from utils.save_and_upload_results import save_and_upload_results

# def lambda_handler(event, context):
def main(event):
    # Initialize boto3 S3 client
    # s3 = boto3.client('s3')
    
    # Retrieve bucket, dataset type, and file paths from the event (use .get() to avoid KeyError)
    bucket = event.get('s3_bucket')
    dataset_type = event.get('dataset_type', 'Pathway')  # Default to 'Pathway'
    # preprocessed_data_s3_key = event.get('preprocessed_data_s3_key')
    # label_file_s3_key = event.get('label_file_s3_key')
    preprocessed_data_local_path = event.get("preprocessed_data_local_path")
    label_file_local_path = event.get('label_file_local_path')

    
    # Handle missing event keys
    # if not bucket or not preprocessed_data_s3_key or not label_file_s3_key:
    #     return {
    #         'statusCode': 400,
    #         'error': 'Missing required parameters in the event: s3_bucket, preprocessed_data_s3_key, or label_file_s3_key.'
    #     }
    
    if not bucket or not preprocessed_data_local_path or not label_file_local_path:
        return {
            'statusCode': 400,
            'error': 'Missing required parameters in the event: s3_bucket, preprocessed_data_local_path, or label_file_local_path.'
        }


    # Define output folder and local file paths based on dataset type
    # if dataset_type == 'Pathway':
    #     output_folder = 'PC_Pathway/analysis_outputs/'
    #     preprocessed_data_local_path = 'PC_Pathway/intermediate/preprocessed_data_pathway.pkl'
    #     label_file_local_path = '/PC_Pathway/pathway_label_list.csv'
    # elif dataset_type == 'Orthology':
    #     output_folder = 'PC_Orthology/analysis_outputs/'
    #     preprocessed_data_local_path = 'PC_Orthology/intermediate/preprocessed_data_orthology.pkl'
    #     label_file_local_path = '/PC_Orthology/orthology_label_list.csv'
    if dataset_type == 'Pathway':
        output_folder = 'PC_Pathway/analysis_outputs/'
    elif dataset_type == 'Orthology':
        output_folder = 'PC_Orthology/analysis_outputs/'

    else:
        return {
            'statusCode': 400,
            'error': f"Unsupported dataset type: {dataset_type}. Please specify 'Pathway' or 'Orthology'."
        }
    
    # Download preprocessed data and label file from S3
        # s3.download_file(bucket, preprocessed_data_s3_key, preprocessed_data_local_path)
        # s3.download_file(bucket, label_file_s3_key, label_file_local_path)

    # Load preprocessed data
    try:
        with open(preprocessed_data_local_path, 'rb') as f:
            data = pickle.load(f)  # Load the preprocessed data (pandas DataFrame)
            data = data.loc[:,~data.columns.duplicated()].copy()
    except Exception as e:
        return {
            'statusCode': 500,
            'error': f"Error loading preprocessed data: {e}"
        }
    
    # Split the data into features and target (assuming 'group_2' as target)
    X_dat = data.iloc[:, 1:]  # Assuming first column is the target variable
    y = data.iloc[:, 0]  # Assuming first column is 'group_2'
    features = X_dat.columns

  
    # Perform Statistical Analysis
    try:
        # Log the control and cancer group data
        data_control = data[data['group_2'] == 0]
        data_cancer = data[data['group_2'] == 1]
        print(f"Control group data: {data_control}")
        print(f"Cancer group data: {data_cancer}")

        # Perform basic statistics
        results = compute_statistics(data_control, data_cancer, features)
        print(f"Results from compute_statistics: {results}")  # Log the results from compute_statistics

        # Perform univariate logistic regression
        results, problematic_features = logistic_regression_univariate_w_BH(data, features, results, alpha=0.05)
        
        # Save and upload results to S3
        results_path = save_and_upload_results(dataset_type,results, label_file_local_path, bucket, output_folder)
    except Exception as e:
        return {
            'statusCode': 500,
            'error': f"Error during statistical analysis: {e}"
        }
    
    # Return success with the path to the results
    return {
        'statusCode': 200,
        'results_path': results_path
    }

event = {
    's3_bucket' : 'local_path',
    "dataset_type": "Orthology",
    "preprocessed_data_local_path": "./PC_Orthology/intermediate/preprocessed_data_orthology.pkl",
    "label_file_local_path": "./PC_Orthology/orthology_label_list.csv"
}

import json
result = main(event)
print(json.dumps(result, indent = 2))
