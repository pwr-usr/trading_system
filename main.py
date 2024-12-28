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
    
    # Update stock information first
    logging.info("Updating stock information...")
    stock_result = manager.update_stock_info()
    if not stock_result['success']:
        logging.error(f"Failed to update stock info: {stock_result.get('error')}")
        return

    # Get list of AH stocks
    ah_stocks = manager.get_ah_stock_list()
    logging.info(f"Found {len(ah_stocks)} AH stocks")

    # Set date range for candlestick data
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")  # Yesterday
    start_date = (datetime.now() - timedelta(days=3650)).strftime("%Y-%m-%d")  # Last 10 years
    
    # Update candlestick data for all AH stocks
    logging.info(f"Updating candlestick data from {start_date} to {end_date}...")
    results = manager.update_ah_stock_data(
        start_date=start_date,
        end_date=end_date,
        adj_type="bc_rights",  # Using 后复权 as default
        force=False  # Don't force update if data exists
    )
    
    # Log results
    logging.info(f"Successfully updated {len(results['success'])} stocks")
    if results['skipped']:
        logging.info(f"Skipped {len(results['skipped'])} stocks (data already exists)")
    if results['errors']:
        logging.error(f"Failed to update {len(results['errors'])} stocks")
        for error in results['errors']:
            logging.error(f"Error updating {error['name']} ({error['hk_code']}/{error['a_code']}): "
                        f"HK: {error.get('hk_error')}, A: {error.get('a_error')}")


if __name__ == "__main__":
    main()