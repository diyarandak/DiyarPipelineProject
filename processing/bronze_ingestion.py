"""
Bronze Ingestion Layer

Reads raw CSV files from data/raw and writes them to HDFS as Apache Iceberg tables.
Performs minimal transformations:
- Converts column names to snake_case
- Applies basic schema inference
- Saves execution metadata (watermark)
"""

import sys
import re
from pathlib import Path
# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from processing.utils import (
    setup_logger,
    load_config,
    load_tables_config,
    create_spark_session,
    save_watermark,
    load_watermark
)

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

logger = setup_logger("BronzeIngestion")

def sanitize_columns(df: DataFrame) -> DataFrame:
    """Convert column names to snake_case and remove invalid characters."""
    def clean_name(name: str) -> str:
        # Replace non-alphanumeric with underscore and make lowercase
        clean = re.sub(r'[^a-zA-Z0-9]', '_', name).lower()
        # Remove consecutive underscores
        clean = re.sub(r'_+', '_', clean).strip('_')
        return clean
    
    for col in df.columns:
        new_name = clean_name(col)
        if col != new_name:
            df = df.withColumnRenamed(col, new_name)
    return df

def process_bronze_layer():
    logger.info("Initializing Bronze Ingestion Layer...")
    
    config = load_config()
    tables = load_tables_config()
    
    # We use our custom function from utils.py to get an Iceberg-ready Spark session
    spark = create_spark_session(config, "Olist-Bronze-Layer")
    
    raw_path = config["paths"]["raw_data"]
    
    # We will use the Iceberg catalog defined in utils.py spark config
    catalog_name = "iceberg_catalog"
    
    # Load existing watermarks to prevent duplicate processing
    watermarks = load_watermark()
    
    for table_conf in tables:
        table_name = table_conf["name"]
        source_file = table_conf["source_file"]
        
        # Check watermark: if already SUCCESS, skip this table
        wm_key = f"bronze.{table_name}"
        if watermarks.get(wm_key, {}).get("status") == "SUCCESS":
            logger.info(f"⏭️ Skipping {table_name}: Already processed successfully in watermark.")
            continue
            
        file_path = f"{raw_path}/{source_file}"
        
        logger.info(f"Processing table: {table_name}")
        
        try:
            # 1. Read raw CSV
            df = spark.read.csv(
                file_path, 
                header=True, 
                inferSchema=True, 
                escape='"'
            )
            
            # 2. Sanitize Column Names (Bronze layer rule: schema validation)
            df = sanitize_columns(df)
            
            # 3. Add ingest timestamp (Metadata tracking)
            df = df.withColumn("_ingested_at", F.current_timestamp())
            
            # 4. Write to Iceberg
            target_table = f"{catalog_name}.bronze.{table_name}"
            
            # Create database/namespace if not exists
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.bronze")
            
            row_count = df.count()
            logger.info(f"Writing {row_count} rows to Iceberg table {target_table}...")
            
            # Iceberg write command
            df.write \
                .format("iceberg") \
                .mode("append") \
                .saveAsTable(target_table)
            
            # 5. Save Watermark (from our utils.py)
            save_watermark(
                table_name=table_name,
                rows_processed=row_count,
                layer="bronze",
                status="SUCCESS"
            )
            logger.info(f"✅ Successfully processed {table_name}.")
            
        except Exception as e:
            logger.error(f"❌ Failed to process {table_name}: {str(e)}")
            save_watermark(
                table_name=table_name,
                rows_processed=0,
                layer="bronze",
                status="FAILED"
            )
            
    logger.info("Bronze Layer ingestion completed.")
    spark.stop()

if __name__ == "__main__":
    process_bronze_layer()
