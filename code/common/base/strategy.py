from abc import ABC, abstractmethod
from typing import Any


class Strategy(ABC):
    @abstractmethod
    def execute(self, *args, **kwargs) -> Any:
        """
        Execute the algorithm defined by the concrete strategy.

        Args:
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            Any: The result of the strategy execution.
        """
        pass


class Context(ABC):
    def __init__(self, strategy: Strategy):
        """
        Initialize the context with a given strategy.

        Args:
            strategy (Strategy): The strategy instance to be used.
        """
        self._strategy = strategy

    def set_strategy(self, strategy: Strategy) -> None:
        """
        Allows changing the strategy at runtime.

        Args:
            strategy (Strategy): The new strategy instance.
        """
        self._strategy = strategy

    def execute_strategy(self, *args, **kwargs) -> Any:
        """
        Executes the current strategy with the provided arguments.

        Args:
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            Any: The result from the strategy's execution.
        """
        return self._strategy.execute(*args, **kwargs)
