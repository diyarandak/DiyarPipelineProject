"""
Verify downloaded Olist dataset CSV files.

Checks if all 9 expected files are present and performs a basic
line count to ensure files are not empty or corrupted.
"""

import sys
from pathlib import Path

# Add project root to path so we can import our modules
sys.path.append(str(Path(__file__).parent.parent))

from processing.utils import setup_logger

logger = setup_logger("VerifyData")

EXPECTED_FILES = {
    "olist_customers_dataset.csv": 99442,
    "olist_geolocation_dataset.csv": 1000164,
    "olist_order_items_dataset.csv": 112651,
    "olist_order_payments_dataset.csv": 103887,
    "olist_order_reviews_dataset.csv": 104720,
    "olist_orders_dataset.csv": 99442,
    "olist_products_dataset.csv": 32952,
    "olist_sellers_dataset.csv": 3096,
    "product_category_name_translation.csv": 72,
}


def verify_dataset(data_dir: str = "data/raw"):
    """Verify that all expected CSV files exist and have the correct line count."""
    logger.info("Starting dataset verification...")
    
    raw_path = Path(data_dir)
    if not raw_path.exists():
        logger.error(f"Data directory not found: {raw_path}")
        sys.exit(1)

    all_passed = True

    for filename, expected_count in EXPECTED_FILES.items():
        file_path = raw_path / filename
        
        if not file_path.exists():
            logger.error(f"MISSING: {filename}")
            all_passed = False
            continue
            
        # Count raw lines in the file
        with open(file_path, 'r', encoding='utf-8') as f:
            actual_count = sum(1 for _ in f)
            
        if actual_count == expected_count:
            logger.info(f"OK: {filename:<40} ({actual_count} rows)")
        else:
            logger.warning(
                f"MISMATCH: {filename:<34} "
                f"Expected {expected_count}, got {actual_count}"
            )
            all_passed = False

    if all_passed:
        logger.info("✅ All 9 files verified successfully. Data is ready for ingestion.")
    else:
        logger.error("❌ Verification failed. Please re-download the dataset.")
        sys.exit(1)


if __name__ == "__main__":
    verify_dataset()
