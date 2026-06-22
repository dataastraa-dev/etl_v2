# core/registry.py and core/interfaces.py
from typing import Any, Callable

class StrategyRegistry:
    _registry = {}

    @classmethod
    def register(cls, name: str) -> Callable:
        def wrapper(wrapped_class):
            cls._registry[name] = wrapped_class
            return wrapped_class
        return wrapper

    @classmethod
    def get(cls, name: str):
        return cls._registry.get(name)

class ITransformer:
    """
    All transformers must return (DataFrame, list_of_anomalies).
    They must NEVER raise unhandled exceptions.
    """
    def transform(self, df: 'pd.DataFrame', config: dict[str, Any]) -> tuple['pd.DataFrame', list[dict]]:
        raise NotImplementedError

class IValidator:
    """
    Validators do not mutate the dataframe. 
    They return a list of anomaly dictionaries.
    """
    def validate(self, df: 'pd.DataFrame', config: dict[str, Any]) -> list[dict]:
        raise NotImplementedError