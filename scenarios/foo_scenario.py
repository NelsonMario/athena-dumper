from scenarios.scenario import IScenario
from builder.query import SimpleQueryBuilder
from lib.thread import ThreadSafeWrapper
from lib.exec import execute, execute_with_dependant

class Scenario(ThreadSafeWrapper, IScenario, SimpleQueryBuilder):
    def create_tasks(self):
        tasks = [
            ["groups", self.default_query_map["groups"]],
            ["users", self.default_query_map["users"]],
        ]
        
        return tasks

    def run(self):
        # Define a thread-safe function using the decorator
        @self._with_lock
        def thread_safe_run():
            self.attributes["group_id"] = ["1"]
            return self.create_tasks()
        
        return thread_safe_run()
    
    def __init__(self):
        super().__init__()
        self.attributes = {
            "group_id": [],
            "user_id": [],
        }
        
        self.default_query_map = {
            "groups": 
                lambda:execute(
                            table_name="group",
                            query=SimpleQueryBuilder("database.group")
                                    .select()
                                    .where("group_id LIKE %s", self.attributes["group_id"])
                                    .build()
                        ),
            "users":
                lambda:execute_with_dependant(
                            scenario=self,
                            dependant_query=SimpleQueryBuilder("database.user")
                                .select()
                                .where("group_id LIKE %s", self.attributes["group_id"])
                                .build(),
                            dependant_columns=["user_id"],
                            callback= 
                                lambda:execute(
                                            table_name="user",
                                            query=SimpleQueryBuilder("database.user")
                                                .select()
                                                .where("id IN (%s)", self.attributes["user_id"])
                                                .build(),
                                        ),
                            callback_column="user_id"
                        ),
            }
    
    