from api.stock_api import StockAPI
from models.stock_info import StockInfoDB
from models.candlestick import CandlestickDB
from config import APIConfig, DBConfig
import logging


class StockDataManager:
    def __init__(self):
        self.api = StockAPI(APIConfig.BASE_URL, APIConfig.TOKEN)
        self.stock_db = StockInfoDB(DBConfig.DB_PATH)
        self.candlestick_db = CandlestickDB(DBConfig.DB_PATH)

    def update_stock_info(self, **params):
        try:
            response = self.api.get_company_info(**params)
            if response.get('code') == 1 and response.get('data'):
                self.stock_db.upsert_stocks(response['data'])
                logging.info(f"Successfully updated {len(response['data'])} stocks")
            else:
                logging.error(f"API returned unexpected response: {response}")
        except Exception as e:
            logging.error(f"Failed to update stock data: {str(e)}")
            raise

    def update_candlestick_data(self, stock_code: str, **params):
        try:
            response = self.api.get_candlestick_data(stock_code, **params)
            if response.get('code') == 1 and response.get('data'):
                self.candlestick_db.upsert_candlesticks(stock_code, response['data'])
                logging.info(f"Successfully updated {len(response['data'])} candlesticks for {stock_code}")
            else:
                logging.error(f"API returned unexpected response: {response}")
        except Exception as e:
            logging.error(f"Failed to update candlestick data: {str(e)}")
            raise

    def get_ah_stock_list(self):
        """Get list of all AH stocks"""
        return self.stock_db.get_ah_stocks()

    def update_ah_stock_data(self):
        """Update data for only AH stocks"""
        raise NotImplementedError()
        # ah_stocks = self.stock_db.get_ah_stocks()
        # for stock_code, name, stock_code_a in ah_stocks:
        #     try:
        #         # Update H-share data
        #         self.update_candlestick_data(stock_code=stock_code, ...)
        #         logging.info(f"Updated H-share data for {name} ({stock_code})")
        #
        #         # You could also fetch A-share data if needed
        #         # self.update_candlestick_data(stock_code=stock_code_a, ...)
        #
        #     except Exception as e:
        #         logging.error(f"Failed to update AH stock {stock_code}: {str(e)}")