from api.stock_api import StockAPI
from models.stock_info import StockInfoDB
from models.candlestick import CandlestickDB
from config import APIConfig, DBConfig
from typing import Dict, Any, Optional
import logging
from datetime import datetime, timedelta
import pandas as pd


class StockDataManager:
    def __init__(self):
        self.api = StockAPI(APIConfig.BASE_URL, APIConfig.TOKEN)
        self.stock_db = StockInfoDB(DBConfig.DB_PATH)
        self.candlestick_db = CandlestickDB(DBConfig.DB_PATH)
        
        # Initialize databases
        self.stock_db.init_database()
        self.candlestick_db.init_database()

    def update_stock_info(self, force: bool = False) -> Dict[str, Any]:
        """
        Update stock information in the database
        Args:
            force: If True, update regardless of last update time
        """
        try:
            response = self.api.get_company_info()
            if response.get('code') == 1 and response.get('data'):
                self.stock_db.upsert_stocks(response['data'])
                count = len(response['data'])
                logging.info(f"Successfully updated {count} stocks")
                return {"success": True, "count": count}
            else:
                error = f"API returned unexpected response: {response}"
                logging.error(error)
                return {"success": False, "error": error}
        except Exception as e:
            error = f"Failed to update stock data: {str(e)}"
            logging.error(error)
            return {"success": False, "error": error}

    def get_stock_info_df(self) -> pd.DataFrame:
        """Get all stock information as a DataFrame"""
        return self.stock_db.get_all_stocks_df()

    def get_ah_stocks_df(self) -> pd.DataFrame:
        """Get AH stock information as a DataFrame"""
        return self.stock_db.get_ah_stocks_df()

    def get_candlesticks_df(self, stock_code: str, adj_type: str = "bc_rights") -> pd.DataFrame:
        """Get candlestick data for a stock as DataFrame"""
        return self.candlestick_db.get_candlesticks_df(stock_code, adj_type)

    def get_ah_candlesticks_df(self, hk_code: str, a_code: str, adj_type: str = "bc_rights") -> Dict[str, pd.DataFrame]:
        """Get candlestick data for both HK and A-share listings of a stock"""
        return self.candlestick_db.get_ah_candlesticks_df(hk_code, a_code, adj_type)

    def get_all_ah_candlesticks_df(self, adj_type: str = "bc_rights") -> Dict[str, Dict[str, pd.DataFrame]]:
        """Get candlestick data for all AH stocks"""
        return self.candlestick_db.get_all_ah_candlesticks_df(self.stock_db, adj_type)

    def get_all_ah_candlesticks_df_ratios(self, adj_type: str = "bc_rights") -> Dict[str, Dict[str, pd.DataFrame]]:
           raise NotImplementedError("This method is not implemented, not to move from Notebooks to managers")
    

    def update_candlestick_data(self, 
                               stock_code: str, 
                               start_date: str,
                               end_date: str,
                               adj_type: str = "bc_rights",
                               force: bool = False) -> Dict[str, Any]:
        """
        Update candlestick data for a stock
        Args:
            stock_code: Stock code
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            adj_type: Adjustment type (bc_rights by default)
            force: If True, update even if data exists
        """
        try:
            # Check if we already have the data
            if not force and self.candlestick_db.has_data_for_date_range(
                stock_code, start_date, end_date, adj_type
            ):
                msg = f"Already have data for {stock_code} from {start_date} to {end_date}"
                logging.info(msg)
                return {"success": True, "message": msg, "cached": True}

            # Determine market type based on stock code length
            market = "hk" if len(stock_code) == 5 else "a"
            
            response = self.api.get_candlestick_data(
                stock_code=stock_code,
                start_date=start_date,
                end_date=end_date,
                adj_type=adj_type,
                market=market
            )
            
            if response.get('code') == 1 and response.get('data'):
                self.candlestick_db.upsert_candlesticks(
                    stock_code=stock_code,
                    candlesticks=response['data'],
                    adj_type=adj_type
                )
                count = len(response['data'])
                logging.info(f"Successfully updated {count} candlesticks for {stock_code}")
                return {"success": True, "count": count, "cached": False}
            else:
                error = f"API returned unexpected response: {response}"
                logging.error(error)
                return {"success": False, "error": error}
        except Exception as e:
            error = f"Failed to update candlestick data for {stock_code}: {str(e)}"
            logging.error(error)
            return {"success": False, "error": error}

    def get_ah_stock_list(self):
        """Get list of all AH stocks"""
        return self.stock_db.get_ah_stocks()

    def update_ah_stock_data(self, 
                            start_date: str,
                            end_date: str,
                            adj_type: str = "bc_rights",
                            force: bool = False) -> Dict[str, Any]:
        """
        Update candlestick data for all AH stocks
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            adj_type: Adjustment type (bc_rights by default)
            force: If True, update even if data exists
        """
        # First update stock info to ensure we have latest AH stock list
        self.update_stock_info()
        
        ah_stocks = self.get_ah_stock_list()
        results = {
            "success": [],
            "errors": [],
            "skipped": []
        }
        
        for stock_code, name, stock_code_a in ah_stocks:
            try:
                # Update H-share data
                hk_result = self.update_candlestick_data(
                    stock_code=stock_code,
                    start_date=start_date,
                    end_date=end_date,
                    adj_type=adj_type,
                    force=force
                )
                
                # Update A-share data
                a_result = self.update_candlestick_data(
                    stock_code=stock_code_a,
                    start_date=start_date,
                    end_date=end_date,
                    adj_type=adj_type,
                    force=force
                )
                
                if hk_result['success'] and a_result['success']:
                    if hk_result.get('cached') and a_result.get('cached'):
                        results['skipped'].append({
                            "name": name,
                            "hk_code": stock_code,
                            "a_code": stock_code_a
                        })
                    else:
                        results['success'].append({
                            "name": name,
                            "hk_code": stock_code,
                            "a_code": stock_code_a
                        })
                else:
                    results['errors'].append({
                        "name": name,
                        "hk_code": stock_code,
                        "a_code": stock_code_a,
                        "hk_error": None if hk_result['success'] else hk_result.get('error'),
                        "a_error": None if a_result['success'] else a_result.get('error')
                    })
                    
            except Exception as e:
                results['errors'].append({
                    "name": name,
                    "hk_code": stock_code,
                    "a_code": stock_code_a,
                    "error": str(e)
                })
        
        return results