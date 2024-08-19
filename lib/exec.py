from concurrent.futures import ThreadPoolExecutor, as_completed
from query.conditions import in_with_regex
from executor.athena import AthenaQueryExecutor
from lib.io import write
from lib.dataframe import convert_results_to_df
from lib.log import setup_logging
from lib.task import Task
import logging


setup_logging()

logger = logging.getLogger(__name__)
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
            logger.exception("Each item in queries should be an instance of ChainedQuery", TypeError)
            raise TypeError
        
        # Get the current query from the ChainedQuery object
        curr_query = chained_query.query
        
        # If there are previous results and they are not empty
        if df is not None and not df.empty:
            values = []
            # Iterate over each column in the DataFrame
            for column in df.columns:
                # Extend the values with values from the current column
                # Convert column values to a list and add them to values
                values.extend(df[column].to_list())
                
            # Update the current query to filter based on these values
            curr_query = curr_query.where(in_with_regex(chained_query.dependant_field, values))
        
        # Execute the current query
        df = execute(curr_query)
        
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

def execute_and_write_in_parallel(tasks, workers, prefix_dir, prefix_filename):
    """
    Executes multiple SQL tasks in parallel and write to local.

    Args:
        tasks (List[List]): A list of SQL query strings to be executed.
        workers (int, optional): Number of workers handling task execution in parallel. Default is 6.
        
    Returns:
        List: The results of the executed tasks.
    """
    workers = workers or 6
    
    # Initialize an empty list to store the logs of task results.
    result_logs = []

    # Check if all items in the tasks list are instances of the Task class.
    # If any item is not a Task, log an exception and raise a TypeError.
    if not all(isinstance(task, Task) for task in tasks):
        logger.exception("Each item in tasks should be an instance of Task", TypeError)
        raise TypeError

    # Use ThreadPoolExecutor to execute the tasks in parallel with the specified number of workers.
    with ThreadPoolExecutor(workers) as executor:
        # Submit the callable functions of the tasks to the executor.
        # Map each future to its corresponding task ID.
        future_to_tasks = {
            executor.submit(task.callable_func): task.id for task in tasks
        }
        
        # Process the results as the futures complete.
        for future in as_completed(future_to_tasks):
            task_id = future_to_tasks[future]  # Get the task ID associated with the completed future.
            try:
                result = future.result()  # Retrieve the result of the completed task.
            except Exception as exc:
                # If an exception occurred during task execution, log it as a failure.
                result_logs.append(f"[FAILED] Task-{task_id} generated an exception: {exc}")
            else:
                # If the task executed successfully, log it as a success.
                result_logs.append(f"[SUCCEEDED] Task-{task_id} executed successfully")
                # Write the result to a file, with a filename based on the task ID and optional prefix.
                write(result, prefix_dir, f"{prefix_filename}_{task_id}" if prefix_filename != "" else f"{task_id}")

    # Return the list of result logs.
    return result_logs
