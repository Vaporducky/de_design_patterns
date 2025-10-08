from abc import ABC, abstractmethod
from typing import Type, TypeVar, Generic

# Create a TypeVar for the generic registry manager class
T = TypeVar('T')


class RegistryManager(ABC, Generic[T]):
    @abstractmethod
    def register(self, name: str, registry: Type[T]):
        pass

    @abstractmethod
    def get_registry(self, name: str) -> Type[T]:
        pass


class Factory(ABC, Generic[T]):
    @abstractmethod
    def create(self, name: str, *args, **kwargs) -> T:
        pass
