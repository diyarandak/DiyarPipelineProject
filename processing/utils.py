"""
Olist Big Data Pipeline — Shared Utilities

Provides:
  - Centralized logging configuration
  - YAML config file loading
  - Spark session builder with Iceberg support
  - Watermark tracking for pipeline monitoring
"""

import json
import logging
import os
import sys
import typing
from datetime import datetime
from pathlib import Path

import yaml

# Monkey-patch for Python 3.13 compatibility with PySpark <= 3.4
import types
if 'typing.io' not in sys.modules:
    fake_module = types.ModuleType('typing.io')
    fake_module.BinaryIO = typing.BinaryIO
    sys.modules['typing.io'] = fake_module
    typing.io = fake_module

# Fix for Java 17+ (DirectByteBuffer memory issues)
# We use PYSPARK_SUBMIT_ARGS to pass these exclusively to the driver,
# preventing them from being sent to Java 8 executors.
java_options = (
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
    "--add-opens=java.base/java.io=ALL-UNNAMED "
    "--add-opens=java.base/java.net=ALL-UNNAMED "
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
    "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
)
os.environ["PYSPARK_SUBMIT_ARGS"] = f"--driver-java-options '{java_options}' pyspark-shell"

# ============================================================
# Logging
# ============================================================

def setup_logger(name: str, log_file: str = "reports/ingestion.log") -> logging.Logger:
    """
    Create a logger that writes to both console and a log file.

    Args:
        name: Logger name (typically __name__ of the calling module).
        log_file: Path to the log file.

    Returns:
        Configured logger instance.
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(str(log_path), encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


# ============================================================
# Configuration
# ============================================================

def load_config(config_path: str = "config/pipeline_config.yaml") -> dict:
    """
    Load pipeline configuration from YAML file.

    Args:
        config_path: Path to the pipeline config YAML file.

    Returns:
        Configuration dictionary.

    Raises:
        FileNotFoundError: If the config file does not exist.
    """
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_tables_config(config_path: str = "config/tables.yaml") -> list:
    """
    Load table definitions from YAML file.

    Args:
        config_path: Path to the tables config YAML file.

    Returns:
        List of table configuration dictionaries.
    """
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Tables config not found: {config_path}")

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    return data.get("tables", [])


# ============================================================
# Spark Session Builder
# ============================================================

def create_spark_session(config: dict = None, app_name: str = None):
    """
    Create a SparkSession with Iceberg support.

    Args:
        config: Pipeline configuration dictionary. If None, loads from default path.
        app_name: Override for the Spark application name.

    Returns:
        Configured SparkSession instance.
    """
    from pyspark.sql import SparkSession

    if config is None:
        config = load_config()

    spark_config = config.get("spark", {})
    name = app_name or spark_config.get("app_name", "Olist-Pipeline")
    master = spark_config.get("master", "local[*]")

    # Fix for Java 17+ (DirectByteBuffer memory issues)
    # Java security manager flag removed for Java 11 compatibility
    java_options = (
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
        "--add-opens=java.base/java.io=ALL-UNNAMED "
        "--add-opens=java.base/java.net=ALL-UNNAMED "
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/java.util=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
        "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
    )

    builder = SparkSession.builder \
        .appName(name) \
        .master(master)

    # Apply additional Spark configurations (Iceberg, etc.)
    extra_config = spark_config.get("config", {})
    for key, value in extra_config.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    return spark


# ============================================================
# Watermark Tracking
# ============================================================

def save_watermark(
    table_name: str,
    rows_processed: int,
    layer: str,
    status: str = "SUCCESS",
    watermark_file: str = "reports/watermark.json",
) -> None:
    """
    Save pipeline execution metadata for tracking and auditing.

    Records the timestamp, row count, layer, and status for each
    table ingestion run. This enables:
      - Monitoring pipeline health
      - Tracking data freshness
      - Supporting future incremental load patterns

    Args:
        table_name: Name of the processed table.
        rows_processed: Number of rows written.
        layer: Pipeline layer (bronze, silver, gold).
        status: Execution status (SUCCESS, FAILED, PARTIAL).
        watermark_file: Path to the watermark JSON file.
    """
    watermark_path = Path(watermark_file)
    watermark_path.parent.mkdir(parents=True, exist_ok=True)

    watermarks = {}
    if watermark_path.exists():
        with open(watermark_path, "r", encoding="utf-8") as f:
            try:
                watermarks = json.load(f)
            except json.JSONDecodeError:
                watermarks = {}

    key = f"{layer}.{table_name}"
    watermarks[key] = {
        "table": table_name,
        "layer": layer,
        "last_run": datetime.now().isoformat(),
        "rows_processed": rows_processed,
        "status": status,
    }

    with open(watermark_path, "w", encoding="utf-8") as f:
        json.dump(watermarks, f, indent=2, ensure_ascii=False)


def load_watermark(watermark_file: str = "reports/watermark.json") -> dict:
    """
    Load watermark data from JSON file.

    Args:
        watermark_file: Path to the watermark JSON file.

    Returns:
        Dictionary of watermark records keyed by "layer.table_name".
    """
    path = Path(watermark_file)
    if not path.exists():
        return {}

    with open(path, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return {}
