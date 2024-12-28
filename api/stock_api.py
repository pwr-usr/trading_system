from .base import BaseAPI
from typing import Dict, Any, Optional
from datetime import datetime, timedelta

class StockAPI(BaseAPI):
    def get_company_info(self, fs_table_type: Optional[str] = None,
                        stock_codes: Optional[list] = None) -> Dict[str, Any]:
        params = {}
        if fs_table_type:
            params['fsTableType'] = fs_table_type
        if stock_codes:
            params['stockCodes'] = stock_codes
        return self._make_request("/hk/company", **params)

    def get_candlestick_data(self, stock_code: str, 
                            start_date: str, 
                            end_date: Optional[str] = None,
                            adj_type: str = "bc_rights",
                            market: str = "hk") -> Dict[str, Any]:
        """
        Get candlestick data for a stock.
        
        Args:
            stock_code: The stock code (5 digits for HK, 6 digits for A-shares)
            start_date: Start date in YYYY-MM-DD format
            end_date: Optional end date in YYYY-MM-DD format
            adj_type: Type of price adjustment. One of:
                        - ex_rights (不复权)
                        - lxr_fc_rights (理杏仁前复权)
                        - fc_rights (前复权)
                        - bc_rights (后复权)
            market: Either "hk" for Hong Kong or "a" for A-shares market
        """
        params = {
            "stockCode": stock_code,
            "type": adj_type,
            "startDate": start_date
        }
        if end_date:
            params["endDate"] = end_date

        endpoint = f"/{market}/company/candlestick"
        return self._make_request(endpoint, **params)

    def get_ah_candlestick_data(self, 
                               hk_code: str,
                               a_code: str,
                               start_date: str,
                               end_date: Optional[str] = None,
                               adj_type: str = "bc_rights") -> Dict[str, Dict[str, Any]]:
        """
        Get candlestick data for both HK and A-share listings of a stock
        
        Returns:
            Dict with 'hk' and 'a' keys containing respective market data
        """
        hk_data = self.get_candlestick_data(hk_code, start_date, end_date, adj_type, "hk")
        a_data = self.get_candlestick_data(a_code, start_date, end_date, adj_type, "a")
        
        return {
            "hk": hk_data,
            "a": a_data
        }

    def download_all_ah_candlesticks(self,
                                   stock_info_db,
                                   candlestick_db,
                                   start_date: str,
                                   end_date: Optional[str] = None,
                                   adj_type: str = "bc_rights") -> Dict[str, Any]:
        """
        Download candlestick data for all stocks that are listed in both A-share and HK markets.
        
        Args:
            stock_info_db: StockInfoDB instance
            candlestick_db: CandlestickDB instance
            start_date: Start date in YYYY-MM-DD format
            end_date: Optional end date in YYYY-MM-DD format
            adj_type: Type of price adjustment (defaults to bc_rights/后复权)
            
        Returns:
            Dict containing success and error information
        """
        ah_stocks = stock_info_db.get_ah_stocks()
        results = {
            "success": [],
            "errors": []
        }
        
        for stock in ah_stocks:
            try:
                # Get HK and A-share data
                data = self.get_ah_candlestick_data(
                    hk_code=stock['stock_code'],
                    a_code=stock['stock_code_a'],
                    start_date=start_date,
                    end_date=end_date,
                    adj_type=adj_type
                )
                
                # Store HK data
                if data['hk'].get('data'):
                    candlestick_db.upsert_candlesticks(
                        stock_code=stock['stock_code'],
                        candlesticks=data['hk']['data'],
                        adj_type=adj_type
                    )
                
                # Store A-share data
                if data['a'].get('data'):
                    candlestick_db.upsert_candlesticks(
                        stock_code=stock['stock_code_a'],
                        candlesticks=data['a']['data'],
                        adj_type=adj_type
                    )
                
                results["success"].append({
                    "name": stock['name'],
                    "hk_code": stock['stock_code'],
                    "a_code": stock['stock_code_a']
                })
                
            except Exception as e:
                results["errors"].append({
                    "name": stock['name'],
                    "hk_code": stock['stock_code'],
                    "a_code": stock['stock_code_a'],
                    "error": str(e)
                })
        
        return results