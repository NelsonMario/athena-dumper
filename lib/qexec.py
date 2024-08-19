from executor.athena import AthenaQueryExecutor
from lib.dataframe import convert_results_to_df
from query.conditions import in_with_regex
import logging

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
    
    for chained_query in queries:
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
