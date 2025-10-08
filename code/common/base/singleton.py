import threading
from abc import ABCMeta


class SingletonMeta(type):
    _instances: dict = {}     # Holds the singleton instances for each class.
    _lock = threading.Lock()  # Ensures thread-safe instantiation.

    def __call__(cls, *args, **kwargs):
        # First check without locking for performance.
        if cls not in cls._instances:
            with cls._lock:
                # Second check within the lock to avoid race conditions.
                if cls not in cls._instances:
                    cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]

    def get_instance(cls, *args, **kwargs):
        """
        Returns the singleton instance.

        This method makes the singleton nature of the class explicit,
        aiding readability and emphasizing that only one instance exists.
        """
        return cls(*args, **kwargs)


# This metaclass combines the features of SingletonMeta and ABCMeta.
class SingletonABCMeta(ABCMeta, SingletonMeta):
    def __new__(cls, name, bases, namespace):
        # Create a new class using the combined metaclasses.
        return super().__new__(cls, name, bases, namespace)
