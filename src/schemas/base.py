from sqlalchemy.orm import declarative_base
import pandas as pd
import json


Base = declarative_base()


def handle_array_columns(
    df: pd.DataFrame, array_columns: list
) -> pd.DataFrame:
    """Convert array-like columns to proper format for PostgreSQL."""
    df = df.copy()
    for col in array_columns:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: x if isinstance(x, list) else [])
    return df


def handle_json_columns(df: pd.DataFrame, json_columns: list) -> pd.DataFrame:
    """Convert dict columns to proper JSON format for PostgreSQL"""
    df = df.copy()
    for col in json_columns:
        if col in df.columns:
            df[col] = df[col].apply(json.dumps)
    return df


def handle_datetime_columns(
    df: pd.DataFrame, datetime_columns: list
) -> pd.DataFrame:
    """Convert datetime columns to UTC."""
    df = df.copy()
    for col in datetime_columns:
        if col in df.columns and not pd.api.types.is_datetime64_any_dtype(
            df[col]
        ):
            df[col] = pd.to_datetime(df[col], utc=True)
    return df
