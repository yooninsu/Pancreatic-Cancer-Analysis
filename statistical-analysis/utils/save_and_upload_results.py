import pandas as pd
import boto3

# def save_and_upload_results(dataset_type, results, label_file, s3_bucket, output_folder, s3):
def save_and_upload_results(dataset_type, results, label_file, s3_bucket=None, output_folder=None, s3=None):
    # Convert columns to numeric types
    numeric_cols = ['Control_mean', 'Cancer_mean', 'FC_value', "log2FC_value", 'Wilcoxon_p', 'LogReg_p_univ', "LogReg_p_fdr"]
    results[numeric_cols] = results[numeric_cols].apply(pd.to_numeric, errors='coerce')
    
    # Read the label data
    label_data = pd.read_csv(label_file)
    if dataset_type == "Pathway":
        label_data = label_data.rename(columns={"pathway_kegg_no": "Pathway_Name"})
    elif dataset_type == "Orthology":
        label_data = label_data.rename(columns={"orthology_names": "Orthology_Name"})
    
    # To merge the pathway together, reset the index of results to combine "KEGG_no" column
    results.reset_index(inplace=True)
    results.rename(columns={'index': 'KEGG_no'}, inplace=True)
    
    # Merge results with label_data on 'KEGG_no'
    combined_results = pd.merge(results, label_data, on='KEGG_no', how='left')
    
    # Reorder columns if needed 
    label_column = "Pathway_Name" if dataset_type == "Pathway" else "Orthology_Name"
    if label_column in combined_results.columns:
        cols = combined_results.columns.tolist()
        cols.insert(1, cols.pop(cols.index(label_column)))
        combined_results = combined_results[cols]
        
    # Save the combined results to an Excel file
    local_file_path = f'./statistical_analysis_results_with_labels_{dataset_type}.xlsx'
    combined_results.to_excel(local_file_path, index=True)
    
    # Upload the local file to the S3 bucket
    # s3.upload_file(local_file_path, s3_bucket, f'{output_folder}/statistical_analysis_results_with_labels_{dataset_type}.xlsx')

        # Upload the local file to the S3 bucket if S3 parameters are provided
    if s3 and s3_bucket and output_folder:
        s3.upload_file(local_file_path, s3_bucket, f'{output_folder}/statistical_analysis_results_with_labels_{dataset_type}.xlsx')
        print(f"File uploaded to s3://{s3_bucket}/{output_folder}/statistical_analysis_results_with_labels_{dataset_type}.xlsx")
    else:
        print("S3 upload skipped. File saved locally as:", local_file_path)

    return local_file_path
