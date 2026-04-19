import threading
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd


@dataclass
class DataStore:
    dataframe: Optional[pd.DataFrame] = None
    schema: list[dict[str, str]] = field(default_factory=list)
    _lock: threading.RLock = field(default_factory=threading.RLock)

    def set_dataframe(self, dataframe: pd.DataFrame) -> None:
        with self._lock:
            self.dataframe = dataframe
            self.schema = [
                {"name": str(name), "type": str(dtype)}
                for name, dtype in dataframe.dtypes.items()
            ]

    def get_dataframe(self) -> Optional[pd.DataFrame]:
        with self._lock:
            if self.dataframe is None:
                return None
            return self.dataframe.copy(deep=True)

    def get_schema(self) -> list[dict[str, str]]:
        with self._lock:
            return list(self.schema)

    def get_columns(self) -> list[str]:
        with self._lock:
            return [item["name"] for item in self.schema]

    def has_data(self) -> bool:
        with self._lock:
            return self.dataframe is not None
