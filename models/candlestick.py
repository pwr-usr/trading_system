from .base import BaseDBManager
from typing import List, Dict, Any
from datetime import datetime

class CandlestickDB(BaseDBManager):
    def init_database(self):
        self.execute_query('''
            CREATE TABLE IF NOT EXISTS candlesticks (
                stock_code TEXT,
                date TEXT,
                open REAL,
                close REAL,
                high REAL,
                low REAL,
                volume INTEGER,
                amount REAL,
                change REAL,
                turnover_rate REAL,
                last_updated TIMESTAMP,
                PRIMARY KEY (stock_code, date)
            )
        ''')

    def upsert_candlesticks(self, stock_code: str, candlesticks: List[Dict[str, Any]]):
        for stick in candlesticks:
            self.execute_query('''
                INSERT OR REPLACE INTO candlesticks (
                    stock_code, date, open, close, high, low,
                    volume, amount, change, turnover_rate, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                stock_code,
                stick.get('date'),
                stick.get('open'),
                stick.get('close'),
                stick.get('high'),
                stick.get('low'),
                stick.get('volume'),
                stick.get('amount'),
                stick.get('change'),
                stick.get('to_r'),
                datetime.now().isoformat()
            ))