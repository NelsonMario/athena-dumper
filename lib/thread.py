from threading import Lock
import logging

logger = logging.getLogger(__name__)

class ThreadSafeWrapper:
    """
    A base class that provides thread safety for its derived classes.
    
    This class uses a threading.Lock to ensure that operations on shared resources
    are thread-safe. Derived classes can use the _with_lock method to wrap their
    methods and ensure that only one thread can execute the method at a time.
    """

    def __init__(self):
        """
        Initializes the ThreadSafeBase class by creating a threading.Lock instance.
        This lock will be used to synchronize access to shared resources.
        """
        self._lock = Lock()

    def _with_lock(self, func):
        """
        A decorator method that ensures the wrapped function is executed with
        the threading lock held, providing thread safety for the operation.
        
        Args:
            func: The function to be wrapped and protected by the lock.
        
        Returns:
            A wrapper function that acquires the lock before calling the original
            function and releases the lock afterward.
        """
        def wrapper(*args, **kwargs):
            with self._lock:
                return func(*args, **kwargs)
        return wrapper
