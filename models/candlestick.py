from .base import BaseDBManager
from typing import List, Dict, Any, Optional
from datetime import datetime
import pandas as pd

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
                adj_type TEXT,
                last_updated TIMESTAMP,
                PRIMARY KEY (stock_code, date, adj_type)
            )
        ''')

    def get_latest_date(self, stock_code: str, adj_type: str = "bc_rights") -> Optional[str]:
        """Get the latest date for which we have data for a stock"""
        result = self.fetch_query('''
            SELECT date 
            FROM candlesticks 
            WHERE stock_code = ? AND adj_type = ?
            ORDER BY date DESC
            LIMIT 1
        ''', (stock_code, adj_type))
        return result[0][0] if result else None

    def has_data_for_date_range(self, stock_code: str, start_date: str, end_date: str, adj_type: str = "bc_rights") -> bool:
        """
        Check if we have complete data for the given date range.
        Returns False if:
        - No data exists
        - Data exists but has gaps in the date range
        - Start date or end date is missing
        """
        if not stock_code or not start_date or not end_date:
            return False
            
        # Get the count of business days between start and end date
        expected_days = self.fetch_query('''
            WITH RECURSIVE dates(date) AS (
                SELECT ?
                UNION ALL
                SELECT date(date, '+1 day')
                FROM dates
                WHERE date < ?
            )
            SELECT COUNT(*) 
            FROM dates
            WHERE strftime('%w', date) NOT IN ('0','6')
        ''', (start_date, end_date))[0][0]

        # Get actual count of records in range
        actual_days = self.fetch_query('''
            SELECT COUNT(*) as count
            FROM candlesticks 
            WHERE stock_code = ? 
            AND adj_type = ?
            AND date >= ?
            AND date <= ?
        ''', (stock_code, adj_type, start_date, end_date))[0][0]

        # Data is complete if we have records for all expected business days
        return actual_days >= expected_days

    def upsert_candlesticks(self, stock_code: str, candlesticks: List[Dict[str, Any]], adj_type: str = "bc_rights"):
        for stick in candlesticks:
            self.execute_query('''
                INSERT OR REPLACE INTO candlesticks (
                    stock_code, date, open, close, high, low,
                    volume, amount, change, turnover_rate, adj_type, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                adj_type,
                datetime.now().isoformat()
            ))

    def get_candlesticks(self, stock_code: str, adj_type: str = "bc_rights") -> List[tuple]:
        """Get candlesticks for a specific stock and adjustment type as tuples"""
        return self.fetch_query('''
            SELECT date, open, close, high, low, volume, amount, change, turnover_rate
            FROM candlesticks 
            WHERE stock_code = ? AND adj_type = ?
            ORDER BY date DESC
        ''', (stock_code, adj_type))

    def get_candlesticks_df(self, stock_code: str, adj_type: str = "bc_rights") -> pd.DataFrame:
        """Get candlesticks for a specific stock and adjustment type as DataFrame"""
        return self.fetch_df('''
            SELECT date, open, close, high, low, volume, amount, change, turnover_rate
            FROM candlesticks 
            WHERE stock_code = ? AND adj_type = ?
            ORDER BY date ASC
        ''', (stock_code, adj_type))

    def get_ah_candlesticks_df(self, hk_code: str, a_code: str, adj_type: str = "bc_rights") -> Dict[str, pd.DataFrame]:
        """Get candlesticks for both HK and A-share listings of a stock"""
        hk_data = self.get_candlesticks_df(hk_code, adj_type)
        a_data = self.get_candlesticks_df(a_code, adj_type)
        
        return {
            "hk": hk_data,
            "a": a_data
        }

    def get_all_ah_candlesticks_df(self, stock_info_db, adj_type: str = "bc_rights") -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Get candlesticks for all AH stocks
        Returns:
            Dict with stock codes as keys and dict of "hk" and "a" DataFrames as values
        """
        ah_stocks = stock_info_db.get_ah_stocks()
        result = {}
        
        for stock_code, name, stock_code_a in ah_stocks:
            result[stock_code] = {
                "name": name,
                "data": self.get_ah_candlesticks_df(stock_code, stock_code_a, adj_type)
            }
        
        return result