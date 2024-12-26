import requests
from typing import Dict, Any
import logging

class BaseAPI:
    def __init__(self, base_url: str, token: str):
        self.base_url = base_url
        self.token = token
        self.session = requests.Session()

    def _make_request(self, endpoint: str, **params) -> Dict[str, Any]:
        url = f"{self.base_url}{endpoint}"
        headers = {"Content-Type": "application/json"}
        default_params = {"token": self.token}
        default_params.update(params)

        try:
            response = self.session.post(
                url,
                headers=headers,
                json=default_params
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"API request failed: {str(e)}")
            raise