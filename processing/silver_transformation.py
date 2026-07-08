"""
Silver Layer Processing

Reads data from Bronze Iceberg tables, applies data quality rules (deduplication,
type casting, null handling), and writes to Silver Iceberg tables.
Supports dynamic partitioning based on YAML config.
"""

import sys
from pathlib import Path
# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from processing.utils import (
    setup_logger,
    load_config,
    load_tables_config,
    create_spark_session,
    save_watermark
)

import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType

logger = setup_logger("SilverCleansing")

from processing.data_quality import check_duplicates, check_nulls, write_to_dlq

def process_silver_layer():
    logger.info("Initializing Silver Cleansing Layer...")
    
    config = load_config()
    tables = load_tables_config()
    spark = create_spark_session(config, "Olist-Silver-Layer")
    catalog_name = "iceberg_catalog"
    
    for table_conf in tables:
        table_name = table_conf["name"]
        logger.info(f"Cleansing table: {table_name}")
        
        try:
            # 1. Read from Bronze
            bronze_table = f"{catalog_name}.bronze.{table_name}"
            try:
                df = spark.table(bronze_table)
            except Exception as e:
                logger.warning(f"Bronze table {bronze_table} not found. Skipping. {e}")
                continue
            
            # 2. Data Quality: Deduplication & Null Checks
            if "key_columns" in table_conf:
                key_cols = table_conf["key_columns"]
                
                # Check for duplicates
                df, duplicate_df = check_duplicates(df, key_cols)
                write_to_dlq(duplicate_df, table_name, "Duplicate Primary Key", catalog_name)
                
                # Check for nulls in primary keys
                df, null_df = check_nulls(df, key_cols)
                write_to_dlq(null_df, table_name, "Null Primary Key", catalog_name)
                    
            # 3. Data Quality: Type Casting (Timestamps)
            ts_col = table_conf.get("timestamp_column")
            if ts_col:
                if ts_col in df.columns:
                    # Cast string to standard Spark Timestamp
                    df = df.withColumn(ts_col, F.to_timestamp(F.col(ts_col)))
                    logger.info(f"  Casted {ts_col} to TimestampType")
                else:
                    logger.warning(f"  Timestamp column {ts_col} not found in {table_name}")
            
            # 4. Partitioning logic extraction
            partition_cols = table_conf.get("partition_by", [])
            if partition_cols and ts_col:
                if "year" in partition_cols:
                    df = df.withColumn("year", F.year(F.col(ts_col)))
                if "month" in partition_cols:
                    df = df.withColumn("month", F.month(F.col(ts_col)))
                logger.info(f"  Added partition columns: {partition_cols}")
                
            # 5. Add Metadata
            df = df.withColumn("_updated_at", F.current_timestamp())
            
            # 6. Write to Silver Iceberg
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.silver")
            target_table = f"{catalog_name}.silver.{table_name}"
            
            row_count = df.count()
            logger.info(f"  Writing {row_count} cleansed rows to {target_table}...")
            
            if partition_cols:
                df = df.sortWithinPartitions(*partition_cols)
                writer = df.write.format("iceberg").mode("overwrite").partitionBy(*partition_cols)
            else:
                writer = df.write.format("iceberg").mode("overwrite")
                
            writer.saveAsTable(target_table)
            
            # 7. Save Watermark
            save_watermark(
                table_name=table_name,
                rows_processed=row_count,
                layer="silver",
                status="SUCCESS"
            )
            logger.info(f"✅ Successfully created silver table {table_name}.")
            
        except Exception as e:
            logger.error(f"❌ Failed to process {table_name}: {str(e)}")
            save_watermark(
                table_name=table_name,
                rows_processed=0,
                layer="silver",
                status="FAILED"
            )
            
    logger.info("Silver Layer cleansing completed.")
    spark.stop()

if __name__ == "__main__":
    process_silver_layer()
