from datetime import datetime
import sqlite3
from typing import Dict, Any, List
import json
import logging


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

    def fetch_query(self, query: str, params: tuple = None):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchall()