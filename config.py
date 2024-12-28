import os
from dataclasses import dataclass

@dataclass
class APIConfig:
    BASE_URL = "https://open.lixinger.com/api"
    TOKEN = "e6cbcdca-f61a-47f6-8b3c-31c0e0a00af6"

@dataclass
class DBConfig:
    DB_PATH = os.path.join(os.path.dirname(__file__), "data", "stocks.db")