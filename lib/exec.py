from concurrent.futures import ThreadPoolExecutor, as_completed
from query.conditions import in_with_regex
from executor.athena import AthenaQueryExecutor
from lib.io import write
from lib.dataframe import convert_results_to_df

class ChainedQuery:
    def __init__(self, query, dependant_field=None):
        """
        Initializes a ChainedQuery instance.

        This class represents a query that may depend on the results of previous queries.
        It allows for chaining multiple queries where each subsequent query can use results
        from the previous ones.

        Args:
            query (Query): The Pypika query to be executed.
            dependant_field (Field, optional): A field from the query results that the
                                                next query will depend on. Default is None.
        """
        self.query = query
        self.dependant_field = dependant_field

def chained_execute(queries):
    """
    Executes a sequence of ChainedQuery objects, passing results as dynamic values to the next query.

    This function runs a series of queries where each query can depend on the results of the previous one.
    It handles the execution and chaining of queries based on their dependencies.

    Args:
        queries (list of ChainedQuery): A list of ChainedQuery objects representing the sequence of queries.

    Returns:
        DataFrame: The result of the final query in the sequence.
    """
    df = None
    result = None
    
    for i, chained_query in enumerate(queries):
        # Ensure each item in queries is an instance of ChainedQuery
        if not isinstance(chained_query, ChainedQuery):
            raise TypeError("Each item in queries should be an instance of ChainedQuery")
        
        # Get the current query from the ChainedQuery object
        curr_query = chained_query.query
        
        # If there are previous results and they are not empty
        if df is not None and not df.empty:
            # Extract values from the 'pass' column of the previous result
            values = df['pass'].tolist()
            # Update the current query to filter based on these values
            curr_query = curr_query.where(in_with_regex(chained_query.dependant_field, values))
        
        # Execute the current query
        result = execute(curr_query)
        
        # If this is not the last query in the sequence, update df to only include the 'pass' column
        if i != len(queries) - 1:
            df = result[["pass"]]
        else:
            # For the last query, keep the full result
            df = result
            
    return df
        

def execute(query):
    """
    Executes an SQL query and writes the result to a specified CSV file.

    This function creates an instance of the AthenaQueryExecutor, executes the given SQL query, 
    waits for the query to complete, and writes the results to a CSV file using the write function. 
    After the operation is complete, it deletes the executor instance.

    Args:
        query (str): The SQL query string to be executed.
    """
    query = query.limit(50).get_sql(quote_char=None)
    
    # Initialize the AthenaQueryExecutor
    executor = AthenaQueryExecutor()

    # Execute the query
    executor.execute_query(query)
    
    # Wait for the query to complete
    executor.wait_for_query_to_complete()
    
    # Get query results and convert to dataframe
    df = convert_results_to_df(executor.get_query_results())
    
    # Clean up the executor instance
    del executor
    
    return df

def execute_and_write_in_parallel(tasks, batch_size, workers, prefix_dir, prefix_filename):
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
                    write(result, prefix_dir, f"{prefix_filename}_{task_id}" if prefix_filename != "" else f"{task_id}")
    return result_logs
