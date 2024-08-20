import time
from enum import Enum
import boto3
import logging

logger = logging.getLogger(__name__)

class Status(Enum):
    SUCCEEDED = 1
    FAILED = 0
    CANCELLED = -1
    
class WorkGroup(Enum):
    POWERUSER = "poweruser"

class AthenaQueryExecutor:
    def __init__(self):
        self.athena_client = boto3.client('athena')
        self.work_group = WorkGroup.POWERUSER.value
        self.query_execution_id = None
        self.query_status = False

    def execute_query(self, query):
        """
        Runs SQL Query on Athena Client, assigning query execution id with athena client result
        
        Args:
            query (str): query for querying in athena
        """
        logger.query(f"[PREPARING] Query: {query}")
        
        response = self.athena_client.start_query_execution(
            QueryString=query,
            WorkGroup=self.work_group
        )
        self.query_execution_id = response['QueryExecutionId']
        
        log = {
            "QueryID": self.query_execution_id,
            "Query": query
        }
        
        logger.info(f"[STARTED] Starting execution : {log}")

    def wait_for_query_to_complete(self, max_attempt = 10, initial_delay = 1):
        """
        Checking the status of query execution by 5 seconds. 
        The response status will be consists of SUCCEEDED, FAILED, and CANCELLED
        
        Args
            max_attempt (int, optional): maximum attempt query execution
            initial_delay (int, optional): initial time delay
        """
        attempt = 0
        delay = 0
        
        while attempt < max_attempt:
            response = self.athena_client.get_query_execution(
                QueryExecutionId=self.query_execution_id
            )
            status = response['QueryExecution']['Status']['State']
            
            log = {
                "QueryID": self.query_execution_id,
            }
            
            logger.info(f"[{status}] Waiting for Query Execution : {log}")
            
            if status in [Status.SUCCEEDED.name, Status.FAILED.name, Status.CANCELLED.name]:
                self.query_status = status == Status.SUCCEEDED.name
                return

            # Using fibonacci backoff
            attempt += 1
            delay = initial_delay
            initial_delay = initial_delay + delay
            time.sleep(delay)
        
        self.query_status = False
            
    def get_query_results(self):
        """
        Get query results
        
        Returns:
            ArraysOfObjects: Result of execution
        """
        response = self.athena_client.get_query_results(
            QueryExecutionId=self.query_execution_id
        )

        return response['ResultSet']['Rows']
