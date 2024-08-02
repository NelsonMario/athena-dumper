import os

def save_results_to_csv(dataframe, output_file):
    """
    Saves the DataFrame to a CSV file.
    
    Args:
        dataframe (DataFrame): The pandas DataFrame to be saved.
        output_file (str): The path where the CSV file will be saved.
    """
    dataframe.to_csv(output_file, index=False)
    print(f"Results saved to {output_file}")
    
def write(dataframe, prefix_dir="", file_name='results.csv'):
    """
    Writes the DataFrame to a CSV file. Creates an output directory if it doesn't exist.
    
    Args:
        dataframe (pd.DataFrame): The pandas DataFrame to be saved.
        prefix_dir (str, optional): The prefix directory to be included in the output path.
        file_name (str, optional): The name of the output CSV file.
    """
    # Define the base directory for the output
    base_dir = "output"
    
    # Construct the file path
    file_path = os.path.join(base_dir, prefix_dir, file_name)
    
    # Create the output directory if it doesn't exist
    os.makedirs(os.path.join(base_dir, prefix_dir), exist_ok=True)
    
    # Save the results to a CSV file
    dataframe.to_csv(f"{file_path}.csv", index=False)
    print(f"[SUCCEEDED] {file_path} has been saved")
        
