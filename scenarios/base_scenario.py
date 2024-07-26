from lib.exec import execute, execute_with_dependant
from generator.database_foo.default import FooQueryGenerator
from lib.thread import ThreadSafeWrapper


class BaseScenario():
    """Base class for all scenarios."""
    def __init__(self):
        self.foo_query_generator = ThreadSafeWrapper(FooQueryGenerator())
        self.default_query_map = {
            "user": 
                lambda: execute(self.foo_query_generator.get_users(), "user"),
            
            "transaction": 
                lambda: execute_with_dependant(
                    query_generator=self.foo_query_generator,
                    dependant_query=self.foo_query_generator.get_users(), 
                    dependant_columns=["id"],
                    callback= lambda: execute(
                        self.foo_query_generator.get_transactions()
                    ),
                    callback_column="user_id",
                ),
            }
    
    def create_tasks(self):
        """Create and return a list of tasks to be executed."""
        raise NotImplementedError("Subclasses should implement this method.")
    
    def run(self):
        """Create and run scenario."""
        raise NotImplementedError("Subclasses should implement this method.")
    