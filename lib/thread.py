from threading import Lock

class ThreadSafeWrapper:
    def __init__(self, object):
        """
        Initialize the ThreadSafeWrapper with an object.

        Args:
            obj (object): The object to be wrapped and synchronized.
        """
        self.object = object
        self.lock = Lock()
        
    def __getattr__(self, name):
        """
        Intercept attribute access and wrap method calls with thread safety.

        Args:
            name (str): The name of the attribute being accessed.
        
        Returns:
            function: A wrapped function that synchronizes access to the method.
        """
        def wrapped(*args, **kwargs):
            # Acquire the lock to ensure thread-safe access to the method
            with self.lock:
                method = getattr(self.object, name) # Get the method from the wrapped object
                return method(*args, **kwargs) # Call the method with arguments
        return wrapped
        
    def set_dependant_attr(self, callback_column_name, callback_column_value):
        """
        Set an attribute on the wrapped object in a thread-safe manner.

        Args:
            callback_column_name (str): The name of the attribute to set.
            callback_column_value (any): The value to set for the attribute.
        """
        with self.lock:
            # Acquire the lock to ensure thread-safe access to the method
            self.object.attributes[callback_column_name] = callback_column_value
