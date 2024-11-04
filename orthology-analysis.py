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

def perform_orthology_analysis(data, features):
    """Perform Orthology-specific analysis"""
    try:
        # Log the control and cancer group data
        data_control = data[data['group_2'] == 0]
        data_cancer = data[data['group_2'] == 1]
        logger.info(f"Control group data (Orthology): {data_control}")
        logger.info(f"Cancer group data (Orthology): {data_cancer}")

        # Perform basic statistics for Orthology
        results = compute_statistics(data_control, data_cancer, features)
        logger.info(f"Results from compute_statistics (Orthology): {results}")

        # Perform univariate logistic regression
        results, problematic_features = logistic_regression_univariate_w_BH(data, features, results, alpha=0.05)
        logger.info(f"Logistic regression results (Orthology): {results}")
        logger.info(f"Problematic features (Orthology): {problematic_features}")
        
        return results
    except Exception as e:
        logger.error(f"Error during Orthology analysis: {e}")
        raise

