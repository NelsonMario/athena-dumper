from lib.thread import ThreadSafeWrapper

class Task(ThreadSafeWrapper):
    def __init__(self, id, callable_func):
        # Initialize the base class (ThreadSafeWrapper) to set up the threading lock.
        super().__init__()
        # Store the id and the callable function provided during initialization.
        self.id = id
        self.callable_func = callable_func
    
    def run(self):
        # Define a nested function that wraps the call to callable_func with a lock.
        @self._with_lock
        def thread_safe_run():
            # Execute the callable function in a thread-safe manner.
            return self.callable_func()
        
        # Call the thread-safe version of the function and return its result.
        return thread_safe_run()
