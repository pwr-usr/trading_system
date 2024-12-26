import logging
from managers import StockDataManager
from datetime import datetime, timedelta


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )


def main():
    setup_logging()
    manager = StockDataManager()

    # Update all stock information including AH status
    manager.update_stock_info()

    # Get list of AH stocks
    ah_stocks = manager.get_ah_stock_list()
    print(ah_stocks)
    # logging.info(f"Found {len(ah_stocks)} AH stocks")
    # for stock_code, name, stock_code_a in ah_stocks:
    #     logging.info(f"AH Stock: {name} (H: {stock_code}, A: {stock_code_a})")
    #
    # # Update specific AH stock data
    # manager.update_candlestick_data(
    #     stock_code="00700",
    #     type="lxr_fc_rights",
    #     start_date=last_year,
    #     end_date=today
    # )


if __name__ == "__main__":
    main()