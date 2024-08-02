class IScenario():
    """Base class for all scenarios."""
    def create_tasks(self):
        """Create and return a list of tasks to be executed."""
        raise NotImplementedError("Subclasses should implement this method.")
    
    def run(self):
        """Create and run scenario."""
        raise NotImplementedError("Subclasses should implement this method.")
    