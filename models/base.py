from datetime import datetime
import sqlite3
from typing import Dict, Any, List, Optional
import json
import logging
import pandas as pd

class BaseDBManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.init_database()

    def init_database(self):
        raise NotImplementedError

    def execute_query(self, query: str, params: tuple = None):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            conn.commit()

    def fetch_query(self, query: str, params: tuple = None) -> List[tuple]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchall()

    def fetch_df(self, query: str, params: tuple = None) -> pd.DataFrame:
        """Execute query and return results as a pandas DataFrame"""
        with sqlite3.connect(self.db_path) as conn:
            if params:
                df = pd.read_sql_query(query, conn, params=params)
            else:
                df = pd.read_sql_query(query, conn)
            return df