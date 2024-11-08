import pandas as pd
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.preprocessing import StandardScaler

def lda_analysis(data,target_column, n_components =1):
        """
    Perform Linear Discriminant Analysis (LDA) on the given dataset.

    Parameters:
    - data (pd.DataFrame): The preprocessed DataFrame containing features and target.
    - target_column (str): The name of the target column for classification.
    - n_components (int): Number of components for dimensionality reduction (default is 1).

    Returns:
    - pd.DataFrame: DataFrame with LDA-transformed features.
    - LinearDiscriminantAnalysis object: The fitted LDA model.
    """
     
