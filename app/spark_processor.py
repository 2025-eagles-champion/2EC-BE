import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, desc, hour, from_unixtime, to_date

class SparkProcessor:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("TransferAnalysis") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.memory", "2g") \
            .getOrCreate()
        self.data_path = os.path.join(os.path.dirname(__file__), "data")

    def get_transfers_df(self, start_date=None, end_date=None, chain_filter=None, limit=None):
        # CSV 파일 로드
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv("파일경로/transfers.csv")
        
        # 타임스탬프를 날짜 형식으로 변환 (밀리초 → 초 → 날짜)
        df = df.withColumn(
            "date", 
            to_date(from_unixtime(col("timestamp") / 1000))
        )
        
        # 날짜 필터링 적용
        if start_date and end_date:
            df = df.filter(col("date").between(start_date, end_date))
        elif start_date:
            df = df.filter(col("date") >= start_date)
        elif end_date:
            df = df.filter(col("date") <= end_date)
        
        # 추가 필터링 옵션 (체인 등)
        if chain_filter:
            df = df.filter(
                (col("fromChain") == chain_filter) | 
                (col("toChain") == chain_filter)
            )
        
        # 결과 제한
        if limit:
            df = df.limit(limit)
            
        return df

    def get_transaction_stats(self):
        df = self.get_transfers_df()
        address_stats = df.groupBy("from_address").agg(
            count("tx_hash").alias("tx_count"),
            sum("value").alias("total_value"),
            avg("value").alias("avg_value")
        ).orderBy(desc("tx_count"))
        return address_stats

    def get_time_pattern_analysis(self):
        df = self.get_transfers_df()
        if "timestamp" in df.columns:
            df = df.withColumn("hour", hour(from_unixtime(col("timestamp"))))
            time_pattern = df.groupBy("hour").agg(
                count("tx_hash").alias("tx_count")
            ).orderBy("hour")
            return time_pattern
        return None

    def get_top_accounts(self, n=10):
        df = self.get_transfers_df()
        top_senders = df.groupBy("from_address").agg(
            count("tx_hash").alias("tx_count"),
            sum("value").alias("total_value")
        ).orderBy(desc("tx_count")).limit(n)
        top_receivers = df.groupBy("to_address").agg(
            count("tx_hash").alias("tx_count"),
            sum("value").alias("total_value")
        ).orderBy(desc("tx_count")).limit(n)
        return {
            "top_senders": top_senders,
            "top_receivers": top_receivers
        }

    def get_data_as_pandas(self, df):
        return df.toPandas()

    def close(self):
        self.spark.stop()
