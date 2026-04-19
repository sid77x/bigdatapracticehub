from typing import Any

import duckdb
import pandas as pd


def execute_sql(query: str, dataframe: pd.DataFrame) -> dict[str, Any]:
    connection = duckdb.connect(database=":memory:")
    try:
        connection.register("data", dataframe)
        result_df = connection.execute(query).fetchdf()
        preview_df = result_df.head(200)
        return {
            "columns": list(preview_df.columns),
            "rows": preview_df.to_dict(orient="records"),
            "row_count": int(len(result_df.index)),
        }
    finally:
        connection.close()
