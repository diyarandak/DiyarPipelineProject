"""
Download the Olist Brazilian E-Commerce dataset from Kaggle.

Requirements:
  - Kaggle account at https://www.kaggle.com
  - API token at ~/.kaggle/kaggle.json  (Windows: %USERPROFILE%\.kaggle\kaggle.json)

Usage:
  python scripts/download_dataset.py
"""

import os
import sys
from pathlib import Path

# Add project root to path so we can import our modules
sys.path.append(str(Path(__file__).parent.parent))

from processing.utils import setup_logger

logger = setup_logger("DownloadDataset")


def check_kaggle_credentials():
    """Check that kaggle credentials exist at the expected location."""
    home = Path.home()
    kaggle_json = home / ".kaggle" / "kaggle.json"
    kaggle_token = home / ".kaggle" / "access_token"
    
    if not (kaggle_json.exists() or kaggle_token.exists() or "KAGGLE_API_TOKEN" in os.environ):
        logger.error("Kaggle API credentials not found.")
        logger.info(f"Expected location: {kaggle_token} OR {kaggle_json}")
        logger.info("\nTo fix:")
        logger.info("  1. Go to https://www.kaggle.com/settings")
        logger.info("  2. Scroll to 'API' and click 'Create New Token'")
        logger.info("  3. Run the provided terminal command to save the token")
        sys.exit(1)


def download_dataset():
    check_kaggle_credentials()

    try:
        import kaggle
    except ImportError:
        logger.info("Installing kaggle package...")
        os.system(f"{sys.executable} -m pip install kaggle")
        import kaggle

    output_dir = Path("data/raw")
    output_dir.mkdir(parents=True, exist_ok=True)

    dataset = "olistbr/brazilian-ecommerce"
    logger.info(f"Downloading dataset: {dataset}")
    logger.info(f"Output directory:    {output_dir.absolute()}")

    kaggle.api.authenticate()
    kaggle.api.dataset_download_files(
        dataset,
        path=str(output_dir),
        unzip=True,
        quiet=False,
    )

    logger.info("Download complete. Files:")
    csv_files = sorted(output_dir.glob("*.csv"))
    if not csv_files:
        logger.warning("No CSV files found — check the output directory.")
        
    for f in csv_files:
        size_mb = f.stat().st_size / (1024 * 1024)
        logger.info(f"  {f.name:<55} {size_mb:6.1f} MB")


if __name__ == "__main__":
    download_dataset()
