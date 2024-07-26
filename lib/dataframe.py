import pandas as pd

def convert_results_to_df(rows):
    """
    Converts the query results into a pandas DataFrame.
    
    Args:
        rows (list): List of rows containing query results.
    
    Returns:
        DataFrame: A pandas DataFrame with the query results.
    """
    if not rows:
        print("No data to write.")
        return

    # Extract headers from the first row
    headers = [col['VarCharValue'] for col in rows[0]['Data']]
    
    # Extract data from subsequent rows
    data_rows = []
    for row in rows[1:]:  # Skip the header row
        data_row = [col.get('VarCharValue') for col in row['Data']]
        data_rows.append(data_row)

    # Create DataFrame with the extracted headers and data
    df = pd.DataFrame(data_rows, columns=headers)
    return df


