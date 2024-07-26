from scenarios.base_scenario import BaseScenario

class Scenario(BaseScenario):
    def create_tasks(self):
        tasks = [
            ["user", self.default_query_map["user"]],
            ["transaction", self.default_query_map["transaction"]],
        ]
        
        return tasks
    
    def run(self):
        self.foo_query_generator.set_dependant_attr("user", "john")
        return self.create_tasks()
    
    