from concurrent.futures import ThreadPoolExecutor, as_completed
from executor.athena import AthenaQueryExecutor
from lib.io import write
from lib.dataframe import convert_results_to_df

def execute_with_dependant(query_generator,
                           dependant_query, 
                           dependant_columns, 
                           callback, 
                           callback_column):
    """
    Executes tasks with dependant, where the second query depends on the results of the first query. 
    waits for the query to complete, and writes the results to a CSV file using the write function.
    After the operation is complete, it deletes the executor instance.
    
    Args:
        query_generator: The generator query responsible to generate query
        dependant_query (str): The SQL query for parent table.
        dependant_column (list): The columns in the parent result. used for the callback.
        callback (func): The function for execute SQL query for child table. this functions will be called after parent query executed
        callback_column (str): The column in the child result.

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
        column_values.extend(parent_df[column].tolist())
            
    # Update callback columns in Query Generator before running the callback query
    query_generator.set_dependant_attr(callback_column, column_values)
        
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

def execute_in_parallel(tasks, batch_size=3):
    """
    Executes multiple SQL tasks in parallel and write to local.

    Args:
        tasks (List[List]): A list of SQL query strings to be executed.
        batch_size (int, optional): Number of workers handling task execution in parallel. Default is 3.
        
    
    Returns:
        List: The results of the executed tasks.
    """
    result_logs = []
    
    for i in range(0, len(tasks), batch_size):
        # Create a batch of tasks with up to batch_size tasks
        batch = tasks[i:i+batch_size]
        # Create a ThreadPoolExecutor to manage parallel execution of tasks
        with ThreadPoolExecutor(batch_size) as executor:
            # Submit each task to the executor and map futures to task IDs
            future_to_tasks = {
                executor.submit(task): task_id for task_id, task in batch
            }
            # Process the results of completed tasks as they finish
            for future in as_completed(future_to_tasks):
                # Retrieve the task ID associated with the completed future
                task_id = future_to_tasks[future]
                try:
                    # Retrieve the result of the completed task
                    result = future.result()
                except Exception as exc:
                     # Log an error message if the task raised an exception
                    result_logs.append(f"Task with {task_id} generated an exception: {exc}")
                else:
                    # Log a success message and handle the result if the task completed successfully
                    result_logs.append(f"Task {task_id} executed successfully")
                    write(result, task_id) # Implement the write function to handle results
    return result_logs
