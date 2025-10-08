import weakref
import threading
import logging
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional, Callable


class Observer(ABC):
    @abstractmethod
    def update(self, subject: "Subject") -> Any:
        """
        Receive an update from the subject.

        Args:
            subject: The subject that triggered the update

        Returns:
            Any: Optional return value
        """
        pass


class Subject(ABC):
    """
    Abstract Subject class for the Observer pattern.

    This class manages observer registration and notification, with thread
    safety and proper resource management.
    """

    def __init__(self, max_workers: Optional[int] = None):
        """
        Initialize the subject.

        Args:
            max_workers: Maximum number of workers for parallel notification.
                         If None, defaults to a reasonable number based on CPU
                         count.
        """
        self._observers = weakref.WeakSet()
        self._lock = threading.Lock()
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._logger = logging.getLogger(self.__class__.__name__)

    def __del__(self):
        """Ensure resources are cleaned up when the subject is destroyed."""
        self.shutdown()

    def shutdown(self):
        """Properly shut down the executor."""
        if hasattr(self, '_executor') and self._executor:
            self._executor.shutdown(wait=False)

    def attach(self, observer: Observer) -> None:
        """
        Attach an observer to this subject.

        Args:
            observer: The observer to attach
        """
        with self._lock:
            self._observers.add(observer)

    def detach(self, observer: Observer) -> None:
        """
        Detach an observer from this subject.

        Args:
            observer: The observer to detach
        """
        with self._lock:
            self._observers.discard(observer)

    def notify(self) -> None:
        """
        Notify all observers of a state change.
        Executes observer updates in parallel using the thread pool.
        """
        # Take a snapshot of observers to ensure thread-safety during iteration
        with self._lock:
            observers_snapshot = list(self._observers)

        # Submit each observer's update method to the thread pool
        futures = [
            self._executor.submit(self._safe_update, observer)
            for observer in observers_snapshot
        ]

        # Wait for all updates to complete
        for future in futures:
            try:
                future.result()
            except Exception as e:
                # This should not happen as exceptions are handled in _safe_update
                self._logger.error(
                    f"Unexpected error in observer notification: {e}"
                )

    def _safe_update(self, observer: Observer) -> None:
        """
        Safely execute an observer's update method, catching and logging any exceptions.

        Args:
            observer: The observer to update
        """
        try:
            observer.update(self)
        except Exception as e:
            self._logger.error(f"Observer update failed: {e}")
            self._handle_observer_error(observer, e)

    def _handle_observer_error(self, observer: Observer,
                               error: Exception) -> None:
        """
        Handle errors that occur during observer updates.
        Override this method to implement custom error handling.

        Args:
            observer: The observer that raised an exception
            error: The exception raised
        """
        pass  # Default implementation does nothing beyond logging

    @abstractmethod
    def state(self) -> Any:
        """
        Abstract getter for state.

        Returns:
            The current state of the subject
        """
        pass
