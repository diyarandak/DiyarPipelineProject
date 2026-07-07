"""
Data Quality Engine

Implements the data quality checks for the Big Data Pipeline.
Checks for nulls in critical columns, identifies duplicates,
and routes rejected records to a Dead Letter Queue (DLQ) Iceberg table.
"""

import sys
from pathlib import Path
from processing.utils import setup_logger

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from typing import List, Tuple
import logging

logger = logging.getLogger("DataQualityEngine")

def check_duplicates(df: DataFrame, key_columns: List[str]) -> Tuple[DataFrame, DataFrame]:
    """
    Separates unique records from duplicates based on key columns.
    Returns (valid_df, invalid_df)
    """
    if not key_columns:
        return df, df.sparkSession.createDataFrame([], df.schema)
        
    valid_df = df.dropDuplicates(subset=key_columns)
    
    # Subtract to find the exact duplicate rows that were dropped
    invalid_df = df.subtract(valid_df)
    
    return valid_df, invalid_df

def check_nulls(df: DataFrame, critical_columns: List[str]) -> Tuple[DataFrame, DataFrame]:
    """
    Filters out rows containing NULLs in critical columns.
    Returns (valid_df, invalid_df)
    """
    if not critical_columns:
        return df, df.sparkSession.createDataFrame([], df.schema)
        
    null_cond = F.col(critical_columns[0]).isNull()
    for col_name in critical_columns[1:]:
        null_cond = null_cond | F.col(col_name).isNull()
        
    valid_df = df.filter(~null_cond)
    invalid_df = df.filter(null_cond)
    
    return valid_df, invalid_df

def write_to_dlq(invalid_df: DataFrame, table_name: str, reason: str, catalog_name: str = "iceberg_catalog"):
    """
    Writes rejected records to the Dead Letter Queue (DLQ).
    Appends them to an Iceberg table specifically for debugging and audits.
    """
    try:
        reject_count = invalid_df.count()
        if reject_count == 0:
            return
            
        invalid_df.cache()
        logger.warning(f"Routing {reject_count} rejected records to DLQ for {table_name}. Reason: {reason}")
        
        # Add metadata for auditing
        dlq_df = invalid_df.withColumn("_dlq_reason", F.lit(reason)) \
                           .withColumn("_dlq_timestamp", F.current_timestamp())
                           
        target_table = f"{catalog_name}.dlq.{table_name}_rejected"
        spark = invalid_df.sparkSession
        
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.dlq")
        
        dlq_df.write \
            .format("iceberg") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(target_table)
            
    except Exception as e:
        logger.error(f"Failed to write to DLQ for {table_name}: {e}")
