import boto3
import pandas as pd
import pickle
import os
import sys
import logging
# Add /var/task/utils to the Python path if needed
sys.path.append(os.path.join(os.getcwd(), "utils"))

# Import the helper functions
from utils.compute_statistics import compute_statistics
from utils.logistic_regression_univariate_w_BH import logistic_regression_univariate_w_BH
from utils.save_and_upload_results import save_and_upload_results

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def perform_pathway_analysis(data, features):
    """Perform Pathway-specific analysis"""
    try:
        # Log the control and cancer group data
        data_control = data[data['group_2'] == 0]
        data_cancer = data[data['group_2'] == 1]
        logger.info(f"Control group data (Pathway): {data_control}")
        logger.info(f"Cancer group data (Pathway): {data_cancer}")

        # Perform basic statistics for Pathway
        results = compute_statistics(data_control, data_cancer, features)
        logger.info(f"Results from compute_statistics (Pathway): {results}")

        # Perform univariate logistic regression
        results, problematic_features = logistic_regression_univariate_w_BH(data, features, results, alpha=0.05)
        logger.info(f"Logistic regression results (Pathway): {results}")
        logger.info(f"Problematic features (Pathway): {problematic_features}")

        return results
    except Exception as e:
        logger.error(f"Error during Pathway analysis: {e}")
        raise


def lambda_handler(event, context):
    logger.info("Lambda function started")
        
    # Initialize boto3 S3 client
    s3 = boto3.client('s3')
    
    # Retrieve bucket, dataset type, and file paths from the event
    bucket = event.get('s3_bucket')
    dataset_type = event.get('dataset_type', 'Pathway')  # Default to 'Pathway'
    preprocessed_data_s3_key = event.get('preprocessed_data_s3_key')
    label_file_s3_key = event.get('label_file_s3_key')
    
    logger.info(f"Received event: {event}")
    
    # Handle missing event keys
    if not bucket or not preprocessed_data_s3_key or not label_file_s3_key:
        logger.error("Missing required parameters in the event")
        return {
            'statusCode': 400,
            'error': 'Missing required parameters in the event: s3_bucket, preprocessed_data_s3_key, or label_file_s3_key.'
        }
    
    # Define output folder and local file paths based on dataset type
    if dataset_type == 'Pathway':
        output_folder = 'PC_Pathway/analysis_outputs/'
        preprocessed_data_local_path = '/tmp/preprocessed_data_pathway.pkl'
        label_file_local_path = '/tmp/pathway_label_list.csv'
        logger.info(f"Pathway analysis selected")
    elif dataset_type == 'Orthology':
        output_folder = 'PC_Orthology/analysis_outputs/'
        preprocessed_data_local_path = '/tmp/preprocessed_data_orthology.pkl'
        label_file_local_path = '/tmp/orthology_label_list.csv'
        logger.info(f"Orthology analysis selected")
    else:
        logger.error(f"Unsupported dataset type: {dataset_type}")
        return {
            'statusCode': 400,
            'error': f"Unsupported dataset type: {dataset_type}. Please specify 'Pathway' or 'Orthology'."
        }
    
    # Download preprocessed data and label file from S3
    try:
        s3.download_file(bucket, preprocessed_data_s3_key, preprocessed_data_local_path)
        s3.download_file(bucket, label_file_s3_key, label_file_local_path)
        logger.info(f"Files downloaded from S3: {preprocessed_data_s3_key}, {label_file_s3_key}")
    except Exception as e:
        logger.error(f"Failed to download preprocessed data from S3: {e}")
        return {
            'statusCode': 500,
            'error': f"Failed to download preprocessed data from S3: {e}"
        }
    
    # Load preprocessed data
    try:
        with open(preprocessed_data_local_path, 'rb') as f:
            data = pickle.load(f)  # Load the preprocessed data (pandas DataFrame)
        logger.info(f"Data loaded successfully")
    except Exception as e:
        logger.error(f"Error loading preprocessed data: {e}")
        return {
            'statusCode': 500,
            'error': f"Error loading preprocessed data: {e}"
        }
    
    # Split the data into features and target (assuming 'group_2' as target)
    X_dat = data.iloc[:, 1:]  # Assuming first column is the target variable
    y = data.iloc[:, 0]  # Assuming first column is 'group_2'
    features = X_dat.columns
    
    # Perform analysis based on dataset type
    try:
        if dataset_type == 'Pathway':
            results = perform_pathway_analysis(data, features)
        elif dataset_type == 'Orthology':
            results = perform_orthology_analysis(data, features)
        
        # Save and upload results to S3
        results_path = save_and_upload_results(results, label_file_local_path, bucket, output_folder, s3)
        logger.info(f"Results saved and uploaded to {results_path}")
    except Exception as e:
        logger.error(f"Error during statistical analysis: {e}")
        return {
            'statusCode': 500,
            'error': f"Error during statistical analysis: {e}"
        }
    
    logger.info("Lambda function completed successfully")
    
    # Return success with the path to the results
    return {
        'statusCode': 200,
        'results_path': results_path
    }

# For local testing
import json

if __name__ == "__main__":
    with open('event.json') as f:
        event = json.load(f)
    
    # Call the lambda_handler function
    result = lambda_handler(event, None)
    print(json.dumps(result, indent=2))
