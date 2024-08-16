import pandas as pd
import json

from concurrent.futures import ThreadPoolExecutor, as_completed
from executor.athena import AthenaQueryExecutor
from lib.io import write
from lib.dataframe import convert_results_to_df

def execute_with_dependant(scenario,
                           dependant_query, 
                           dependant_columns, 
                           callback, 
                           callback_column,
                           is_in_json_format = False):
    """
    Executes tasks with dependant, where the second query depends on the results of the first query. 
    waits for the query to complete, and writes the results to a CSV file using the write function.
    After the operation is complete, it deletes the executor instance.
    
    Args:
        query_generator: The generator query responsible to generate query
        dependant_query (str): The SQL query for parent table.
        dependant_column (list): The columns in the parent result. this attributes will inject the values to callback column.
        callback (func): The function for execute SQL query for child table. this functions will be called after parent query executed
        callback_column (str): This column will reference for updating dictionary value in scenario. 
            it will be useful for querying in child tables that have dependant to column of parent result. 
        is_in_json_format (str, optional): This flag for determine the column of parent result whether in json format or not
        

    Returns:
        DataFrame: The result of the child query.
    """
    
    executor = AthenaQueryExecutor()
    
    # Running the dependant query
    executor.execute_query(dependant_query)
    
    # Wait for the query to complete
    executor.wait_for_query_to_complete()
    
    # Write the results to the specified output file
    parent_result = executor.get_query_results()
    parent_df = convert_results_to_df(parent_result)
    
    column_values = []

    for column in dependant_columns:
        if is_in_json_format:
            # Extracting data from JSON in the additional data column
            parent_df[column] = parent_df["additional_data"].apply(
                lambda x: json.loads(x) if pd.notnull(x) else {}
            )
            column_values.extend(parent_df[column].apply(lambda x: str(x.get(column))).tolist())
        else:
            column_values.extend(parent_df[column].tolist())
            
    # Update callback columns in Query Generator before running the callback query
    scenario.set_dependant_attr(callback_column, column_values)
        
    df = callback()
    
    # Clean up the executor instance
    del executor
    
    return df
   

def execute(query, table_name=""):
    """
    Executes an SQL query and writes the result to a specified CSV file.

    This function creates an instance of the AthenaQueryExecutor, executes the given SQL query, 
    waits for the query to complete, and writes the results to a CSV file using the write function. 
    After the operation is complete, it deletes the executor instance.

    Args:
        query (str): The SQL query string to be executed.
        table_name (str, optional): The name of the dataframe file.
    """
    # Initialize the AthenaQueryExecutor
    executor = AthenaQueryExecutor()
    
    # Execute the query
    executor.execute_query(query)
    
    # Wait for the query to complete
    executor.wait_for_query_to_complete()
    
    # Get query results and convert to dataframe
    df = convert_results_to_df(executor.get_query_results())
    df.Name = table_name
    
    # Clean up the executor instance
    del executor
    
    return df

def execute_and_write_in_parallel(tasks, batch_size, workers, prefix_dir):
    """
    Executes multiple SQL tasks in parallel and write to local.

    Args:
        tasks (List[List]): A list of SQL query strings to be executed.
        workers (int, optional): Number of workers handling task execution in parallel. Default is 3.
        batch_size (int, optional): Number of query processed together in one pass. Default is 3.
        
    
    Returns:
        List: The results of the executed tasks.
    """
    batch_size = batch_size or 3
    workers = workers or 3
    
    result_logs = []
    
    for i in range(0, len(tasks), batch_size):
        batch = tasks[i:i+batch_size]
        with ThreadPoolExecutor(workers) as executor:
            future_to_tasks = {
                executor.submit(task): task_id for task_id, task in batch
            }
            for future in as_completed(future_to_tasks):
                task_id = future_to_tasks[future]
                try:
                    result = future.result()
                except Exception as exc:
                    result_logs.append(f"Task with {task_id} generated an exception: {exc}")
                else:
                    result_logs.append(f"Task {task_id} executed successfully")
                    write(result, prefix_dir, task_id)
    return result_logs
