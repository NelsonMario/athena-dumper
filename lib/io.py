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
    
def write(dataframe, output_file='results.csv'):
    """
    Writes the query results to a CSV file if the query is successful.
    
    Args:
        dataframe (Dataframe): The pandas DataFrame to be saved.
        output_file (str, optional): The name of the output CSV file.
    """
        
    # Create the output directory if it doesn't exist
    os.makedirs("output", exist_ok=True)
    
    # Save the results to a CSV file in the output directory
    save_results_to_csv(
        dataframe=dataframe, 
        output_file=f"output/{output_file}.csv"
    )
        
