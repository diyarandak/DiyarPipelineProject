"""
Gold Layer Processing (Star Schema Modeling)

Reads cleansed data from Silver Iceberg tables and models them into a Kimball Star Schema
(Fact and Dimension tables) optimized for Business Intelligence and Analytics.
Writes the final tables to Gold Iceberg tables.
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from processing.utils import (
    setup_logger,
    load_config,
    create_spark_session,
    save_watermark
)

import pyspark.sql.functions as F

logger = setup_logger("GoldModeling")

def process_gold_layer():
    logger.info("Initializing Gold Modeling Layer (Star Schema)...")
    
    config = load_config()
    spark = create_spark_session(config, "Olist-Gold-Layer")
    catalog_name = "iceberg_catalog"
    
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.gold")
    
    try:
        # ==========================================
        # 1. LOAD SILVER TABLES
        # ==========================================
        logger.info("Loading Silver tables...")
        df_orders = spark.table(f"{catalog_name}.silver.olist_orders_dataset")
        df_items = spark.table(f"{catalog_name}.silver.olist_order_items_dataset")
        df_reviews = spark.table(f"{catalog_name}.silver.olist_order_reviews_dataset")
        df_payments = spark.table(f"{catalog_name}.silver.olist_order_payments_dataset")
        df_customers = spark.table(f"{catalog_name}.silver.olist_customers_dataset")
        df_sellers = spark.table(f"{catalog_name}.silver.olist_sellers_dataset")
        df_products = spark.table(f"{catalog_name}.silver.olist_products_dataset")
        df_translation = spark.table(f"{catalog_name}.silver.product_category_name_translation")
        df_geo = spark.table(f"{catalog_name}.silver.olist_geolocation_dataset")

        # ==========================================
        # 2. BUILD DIMENSION TABLES
        # ==========================================
        logger.info("Building Dimension Tables...")
        
        # DIM_CUSTOMERS
        dim_customers = df_customers.select(
            "customer_id", 
            "customer_unique_id",
            "customer_zip_code_prefix", 
            "customer_city", 
            "customer_state"
        ).withColumn("_updated_at", F.current_timestamp())
        
        # DIM_SELLERS
        dim_sellers = df_sellers.select(
            "seller_id", 
            "seller_zip_code_prefix", 
            "seller_city", 
            "seller_state"
        ).withColumn("_updated_at", F.current_timestamp())
        
        # DIM_PRODUCTS (Joined with translation)
        dim_products = df_products.join(
            df_translation, 
            on="product_category_name", 
            how="left"
        ).select(
            "product_id",
            F.col("product_category_name_english").alias("product_category_name"),
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm"
        ).withColumn("_updated_at", F.current_timestamp())

        # DIM_GEOLOCATION (Aggregated to resolve multiple coords per zip code)
        dim_geolocation = df_geo.groupBy("geolocation_zip_code_prefix").agg(
            F.avg("geolocation_lat").alias("avg_latitude"),
            F.avg("geolocation_lng").alias("avg_longitude"),
            F.first("geolocation_city").alias("city"),
            F.first("geolocation_state").alias("state")
        ).withColumn("_updated_at", F.current_timestamp())

        # DIM_DATES (Time Dimension)
        dim_dates = spark.sql("""
            SELECT 
                CAST(date_col AS DATE) as date_sk,
                date_col as full_date,
                YEAR(date_col) as year,
                MONTH(date_col) as month,
                DAY(date_col) as day,
                QUARTER(date_col) as quarter,
                DAYOFWEEK(date_col) as day_of_week,
                CASE WHEN DAYOFWEEK(date_col) IN (1, 7) THEN True ELSE False END as is_weekend
            FROM (
                SELECT explode(sequence(to_date('2016-01-01'), to_date('2019-12-31'), interval 1 day)) as date_col
            )
        """).withColumn("_updated_at", F.current_timestamp())

        # ==========================================
        # 3. BUILD FACT TABLES
        # ==========================================
        logger.info("Building Fact Tables...")
        
        # FACT_ORDER_PAYMENTS
        fact_order_payments = df_orders.select(
            "order_id", "customer_id", "order_status", "order_purchase_timestamp", "year", "month"
        ).join(
            df_payments,
            on="order_id",
            how="inner"
        ).withColumn("_updated_at", F.current_timestamp())
        
        # FACT_ORDER_SALES
        # Pre-aggregate reviews to 1 row per order to avoid row explosion
        avg_reviews = df_reviews.groupBy("order_id").agg(
            F.avg("review_score").alias("avg_review_score")
        )
        
        fact_order_sales = df_items.join(
            df_orders.select(
                "order_id", "customer_id", "order_status", 
                "order_purchase_timestamp", "order_approved_at",
                "order_delivered_customer_date", "order_estimated_delivery_date",
                "year", "month"
            ),
            on="order_id",
            how="inner"
        ).join(
            avg_reviews,
            on="order_id",
            how="left"
        ).withColumn("_updated_at", F.current_timestamp())

        # ==========================================
        # 4. WRITE TO GOLD ICEBERG
        # ==========================================
        logger.info("Writing modeled tables to Gold Layer...")
        
        def write_gold_table(df, table_name, partition_cols=None):
            target = f"{catalog_name}.gold.{table_name}"
            row_count = df.count()
            logger.info(f"  Writing {row_count} rows to {target}...")
            
            if partition_cols:
                df = df.sortWithinPartitions(*partition_cols)
                writer = df.write.format("iceberg").mode("overwrite").partitionBy(*partition_cols)
            else:
                writer = df.write.format("iceberg").mode("overwrite")
            writer.saveAsTable(target)
            
            save_watermark(table_name=table_name, rows_processed=row_count, layer="gold", status="SUCCESS")
            return True

        # Write Dimensions
        write_gold_table(dim_customers, "dim_customers")
        write_gold_table(dim_sellers, "dim_sellers")
        write_gold_table(dim_products, "dim_products")
        write_gold_table(dim_geolocation, "dim_geolocation")
        write_gold_table(dim_dates, "dim_dates")
        
        # Write Facts (Partitioned)
        write_gold_table(fact_order_payments, "fact_order_payments", partition_cols=["year", "month"])
        write_gold_table(fact_order_sales, "fact_order_sales", partition_cols=["year", "month"])
        
        logger.info("✅ Gold Layer modeling completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Failed in Gold Layer processing: {str(e)}")
        save_watermark(table_name="gold_layer_batch", rows_processed=0, layer="gold", status="FAILED")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    process_gold_layer()
