import pandas as pd
import statsmodels.api as sm
import warnings
import numpy as np
from statsmodels.tools.sm_exceptions import PerfectSeparationError
from statsmodels.stats.multitest import multipletests  # For FDR correction
def clean_p_values(p_values):
    # Convert the list to a NumPy array for easier manipulation
    p_values = np.array(p_values)
    
    # Remove NaN values
    valid_p_values = p_values[~np.isnan(p_values)]
    
    # Remove infinite values or extremely small p-values
    valid_p_values = valid_p_values[np.isfinite(valid_p_values)]
    
    # If the valid p-values list is empty, return early to avoid errors
    if len(valid_p_values) == 0:
        print("No valid p-values to apply FDR correction.")
        return None
    
    return valid_p_values

def logistic_regression_univariate_w_BH(data, pathways, results,alpha = 0.05):
    problematic_pathways = []
    p_values = [] # List to store all p-values
    pathways_valid = []  # List to store pathways with valid p-values
    
    # Logistic regression univariate
    y = data.iloc[:, 0]  # Assuming first column is 'group_2'
    
    for pathway in pathways:
        X_univ = data[[pathway]]
        X_univ = sm.add_constant(X_univ)  # Add intercept term
        
        # Check for constant predictor
        if X_univ[pathway].nunique() <= 1 or X_univ[pathway].isnull().all():
            p_univ = np.nan
            problematic_pathways.append(pathway)
            results.loc[pathway, 'LogReg_p_univ'] = p_univ
            continue  # skip to the next pathway
        
        try:
            # Suppress warnings in this specific block
            with warnings.catch_warnings():
                warnings.simplefilter('ignore', PerfectSeparationError)
                warnings.simplefilter('ignore', np.linalg.LinAlgError)
                warnings.simplefilter('ignore', ValueError)
                
                # Fit the logistic regression model
                model_univ = sm.Logit(y, X_univ)
                result_univ = model_univ.fit(disp=0, maxiter=100)
                p_univ = result_univ.pvalues[1]  # p-value for the pathway predictor (index 1)
                p_values.append(p_univ) # collect p-value
                pathways_valid.append(pathway) # Keep track of valid pathways
        except (np.linalg.LinAlgError, ValueError, PerfectSeparationError) as e:
            p_univ = np.nan  # Indicate invalid result
            problematic_pathways.append(pathway)
            print(f"Pathway '{pathway}' encountered an error: {e}")

        # Store the result
        results.loc[pathway, 'LogReg_p_univ'] = p_univ



    # Clean the p-values before applying FDR correction
    p_values_cleaned = clean_p_values(p_values)

# Apply FDR correction (Benjamini-Hochberg method)
    if p_values:
        print(f"Applying FDR correction to p-values: {p_values}")
        _, p_values_fdr, _, _ = multipletests(p_values_cleaned, alpha=alpha, method='fdr_bh')  # FDR correction
        print(f"FDR-adjusted p-values: {p_values_fdr}")
        
        # Store FDR-adjusted p-values back in the results DataFrame
        for pathway, p_fdr in zip(pathways_valid, p_values_fdr):
            results.loc[pathway, 'LogReg_p_fdr'] = p_fdr  # FDR-corrected p-values
    else:
        print("No valid p-values collected; skipping FDR correction.")
    
    # Debugging: Check the final results DataFrame
    print(f"Final results with p-values and FDR corrections:\n{results}")
    
    return results, problematic_pathways