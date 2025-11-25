import pandas as pd
import logging

logger = logging.getLogger(__name__)

def ensure_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame()
    return df

def empty_df_with_columns(columns):
    return pd.DataFrame(columns=columns)

def log_transform_info(table_name: str, input_rows: int, output_rows: int):
    """Log simple para transformaciones"""
    logger.info(f"{table_name}: {input_rows} → {output_rows} registros procesados")
