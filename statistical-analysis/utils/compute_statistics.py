import pandas as pd
import numpy as np
from scipy.stats import mannwhitneyu

def compute_statistics(data_control, data_cancer, pathways):
    # Initialize the results DataFrame
    results = pd.DataFrame(index=pathways, columns=[
        'Control_mean', 'Cancer_mean', 'FC_value', 'Wilcoxon_p'
    ])

    for pathway in pathways:
        control_values = data_control[pathway]
        cancer_values = data_cancer[pathway]

        # Ensure the values are numeric
        control_values = pd.to_numeric(control_values, errors='coerce')
        cancer_values = pd.to_numeric(cancer_values, errors='coerce')

        control_mean = control_values.mean()
        cancer_mean = cancer_values.mean()

        # Calculate fold change
        fc_value = (cancer_mean / control_mean) if control_mean != 0 else np.nan
        log2fc_value = np.log2(fc_value) if fc_value > 0 and not np.isnan(fc_value) else np.nan

        # Store the means and fold change
        results.loc[pathway, 'Control_mean'] = control_mean
        results.loc[pathway, 'Cancer_mean'] = cancer_mean
        results.loc[pathway, 'FC_value'] = fc_value
        results.loc[pathway, "log2FC_value"] = log2fc_value

        # Perform Wilcoxon rank-sum test (Mann-Whitney U test)
        try:
            _, p_value = mannwhitneyu(cancer_values, control_values, alternative='two-sided')
        except ValueError:
            p_value = np.nan
        results.loc[pathway, 'Wilcoxon_p'] = p_value

    return results
