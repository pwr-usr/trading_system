from .base import BaseAPI
from typing import Dict, Any, Optional

class StockAPI(BaseAPI):
    def get_company_info(self, fs_table_type: Optional[str] = None,
                        stock_codes: Optional[list] = None) -> Dict[str, Any]:
        params = {}
        if fs_table_type:
            params['fsTableType'] = fs_table_type
        if stock_codes:
            params['stockCodes'] = stock_codes
        return self._make_request("/hk/company", **params)

    def get_candlestick_data(self, stock_code: str, stock_type: str,
                            start_date: str, end_date: Optional[str] = None) -> Dict[str, Any]:
        params = {
            "stockCode": stock_code,
            "type": stock_type,
            "startDate": start_date
        }
        if end_date:
            params["endDate"] = end_date
        return self._make_request("/hk/company/candlestick", **params)