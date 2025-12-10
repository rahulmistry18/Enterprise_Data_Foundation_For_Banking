# In file: scripts/spark_jobs/transformer.py

from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import DecimalType
import os

class SparkTransformer:
    def __init__(self, spark, env="local"):
        self.spark = spark
        # Source path is relative to the Airflow container mount
        self.source_path = os.path.join(os.environ['AIRFLOW_HOME'], 'source_data')
        self.env = env
        print(f"SparkTransformer initialized in {self.env} mode.")

    def load_raw_transactions(self):
        """Loads raw transaction data from CSV."""
        tx_path = os.path.join(self.source_path, 'transaction_data.csv')
        
        df = self.spark.read.csv(tx_path, header=True, inferSchema=True)
        
        # Clean up column types and prepare for calculations
        df = df.withColumn("amount", F.col("amount").cast(DecimalType(18, 2))) \
               .withColumn("transaction_date", F.to_date(F.col("transaction_date"), 'yyyy-MM-dd'))
        
        return df.select("transaction_id", "account_id", "transaction_date", "amount", "type", "source_channel")

    def transform_transactions(self, transactions_df):
        """
        Applies data quality and calculated fields using PySpark.
        """
        
        # Data Quality Check: Filter out transactions with null amounts or zero amounts (Example)
        transactions_df = transactions_df.filter(F.col("amount").isNotNull() & (F.col("amount") > 0))
        print(f"Passed Data Quality: {transactions_df.count()} records remaining after null/zero check.")

        # Determine the sign of the amount: positive for DEPOSIT/TRANSFER, negative for WITHDRAWAL/PURCHASE
        transactions_df = transactions_df.withColumn(
            "signed_amount",
            F.when(F.col("type").isin(["DEPOSIT", "TRANSFER"]), F.col("amount"))
             .otherwise(F.col("amount") * -1)
        )

        # 1. Define the Window Partition: Group by account_id, ordered by date
        account_window = Window.partitionBy("account_id").orderBy("transaction_date")

        # 2. Calculate the Cumulative Balance Change (Key Feature)
        # Demonstrates advanced window function usage
        transactions_df = transactions_df.withColumn(
            "cumulative_balance_change",
            F.sum("signed_amount").over(account_window)
        )

        # Select and rename final columns for the Fact Table Staging
        fact_df = transactions_df.select(
            F.col("transaction_id"),
            F.col("account_id"), 
            F.col("transaction_date"),
            F.col("amount").alias("transaction_amount"),
            F.col("type").alias("transaction_type"),
            F.col("source_channel"),
        )
        return fact_df

    def run_pipeline(self):
        """Orchestrates the Spark jobs."""
        raw_tx_df = self.load_raw_transactions()
        
        # Apply transformation and enrichment
        fact_tx_df = self.transform_transactions(raw_tx_df)
        
        print("--- FACT TRANSACTION SCHEMA ---")
        fact_tx_df.printSchema()
        print("--- FACT TRANSACTION SAMPLE (5 rows) ---")
        fact_tx_df.show(5, truncate=False)
        
        return fact_tx_df