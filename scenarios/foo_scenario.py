from scenarios.scenario import IScenario  
from lib.thread import ThreadSafeWrapper  
from lib.exec import execute, chained_execute, ChainedQuery  
from lib.task import Task
from schema.database_bar import tables as bar  
from query.conditions import in_with_regex  
from query.fields import field  
from pypika import Query

# Define the Scenario class that inherits from ThreadSafeWrapper and IScenario
class Scenario(ThreadSafeWrapper, IScenario):
    # Method to create tasks for execution
    def create_tasks(self):
        # Define tasks as a list of tuples with task names and their corresponding queries
        tasks = [
            Task("groups", self.default_query_map["groups"]),
            Task("users", self.default_query_map["users"]),
        ]
        
        return tasks

    # Method to run the scenario
    def run(self):
        # Set query parameter for 'group_id'
        self.query_param["group_id"] = ["1"]
        # Create tasks using the defined method
        return self.create_tasks()
        
    
    # Constructor for the Scenario class
    def __init__(self):
        # Initialize the parent class
        super().__init__()
        # Initialize query parameters with an empty list for 'group_id'
        self.query_param = {
            "group_id": []
        }
        
        # Define the default query map with lambdas for each query
        self.default_query_map = {
            # Query to fetch all records from the 'groups' table with regex condition
            "groups": 
                lambda: execute(Query
                                .from_(bar.groups)  # Specify the 'groups' table
                                .select("*")  # Select all columns
                                .where(in_with_regex(field(col="group_id"), self.query_param["group_id"]))
                            ),          
            
            # Chained query to fetch user details based on group IDs
            "users": 
                lambda: chained_execute([
                        # First query to get 'user_id' from the 'groups' table
                        ChainedQuery(Query
                            .from_(bar.groups)  # Specify the 'groups' table
                            .select(field("user_id", alias="pass"))  # Select 'user_id' and alias it as 'pass'
                            .where(in_with_regex(field(col="group_id"), self.query_param["group_id"]))),
                        
                        # Second query to get all records from the 'users' table using 'user_id' from the first query
                        ChainedQuery(Query
                            .from_(bar.users)  # Specify the 'users' table
                            .select("*"), field("user_id"))  # Select all columns from 'users' table using 'user_id' for filtering
                        ]
                    ),
        }
