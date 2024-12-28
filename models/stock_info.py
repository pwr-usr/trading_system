from .base import BaseDBManager
from typing import List, Dict, Any
from datetime import datetime
import json
import pandas as pd

class StockInfoDB(BaseDBManager):
    def init_database(self):
        self.execute_query('''
            CREATE TABLE IF NOT EXISTS stocks (
                stock_code TEXT PRIMARY KEY,
                name TEXT,
                exchange TEXT,
                market TEXT,
                ipo_date TEXT,
                area_code TEXT,
                fs_table_type TEXT,
                mutual_markets TEXT,
                listing_status TEXT,
                fs_type TEXT,
                stock_code_a TEXT,
                is_ah BOOLEAN,
                last_updated TIMESTAMP
            )
        ''')

    def upsert_stocks(self, stocks: List[Dict[str, Any]]):
        for stock in stocks:
            mutual_markets = json.dumps(stock.get('mutualMarkets', []))
            stock_code_a = stock.get('stockCodeA', '')
            is_ah = bool(stock_code_a)  # True if stockCodeA exists and not empty

            self.execute_query('''
                INSERT OR REPLACE INTO stocks (
                    stock_code, name, exchange, market, ipo_date,
                    area_code, fs_table_type, mutual_markets,
                    listing_status, fs_type, stock_code_a, is_ah,
                    last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                stock.get('stockCode'),
                stock.get('name'),
                stock.get('exchange'),
                stock.get('market'),
                stock.get('ipoDate'),
                stock.get('areaCode'),
                stock.get('fsTableType'),
                mutual_markets,
                stock.get('listingStatus'),
                stock.get('fsType'),
                stock_code_a,
                is_ah,
                datetime.now().isoformat()
            ))

    def get_ah_stocks(self) -> List[tuple]:
        """Get all AH stocks as a list of tuples"""
        return self.fetch_query('''
            SELECT stock_code, name, stock_code_a 
            FROM stocks 
            WHERE is_ah = 1
        ''')

    def get_all_stocks_df(self) -> pd.DataFrame:
        """Get all stocks information as a DataFrame"""
        return self.fetch_df('''
            SELECT * FROM stocks
        ''')

    def get_ah_stocks_df(self) -> pd.DataFrame:
        """Get all AH stocks as a DataFrame"""
        return self.fetch_df('''
            SELECT * FROM stocks 
            WHERE is_ah = 1
        ''')