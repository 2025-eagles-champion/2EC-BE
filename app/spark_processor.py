from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, desc
import os
from .env import load_dotenv

load_dotenv()

class SparkProcessor:
    def __init__(self):
        # Spark 세션 초기화 (로컬 모드로 시작)
        self.spark = SparkSession.builder \
            .appName("TransferAnalysis") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.memory", "4g") \
            .getOrCreate()
        
        # CSV 파일 경로
        self.data_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
        
    def get_transfers_df(self, limit=None):
        """
        CSV 파일을 PySpark DataFrame으로 로드
        """
        # 디렉토리에서 CSV 파일 찾기
        csv_files = [f for f in os.listdir(self.data_path) if f.startswith('transfers_')]
        
        if not csv_files:
            raise FileNotFoundError("No transfer CSV files found in data directory")
        
        # 가장 최근 파일 사용 (파일명이 타임스탬프를 포함한다고 가정)
        latest_file = sorted(csv_files)[-1]
        file_path = os.path.join(self.data_path, latest_file)
        
        # CSV 파일 로드
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(file_path)
        
        # 필요한 경우 데이터 제한
        if limit:
            df = df.limit(limit)
            
        return df
    
    def get_transaction_stats(self):
        """
        트랜잭션 관련 통계 생성
        """
        df = self.get_transfers_df()
        
        # 주소별 통계
        address_stats = df.groupBy("from_address").agg(
            count("tx_hash").alias("tx_count"),
            sum("value").alias("total_value"),
            avg("value").alias("avg_value")
        ).orderBy(desc("tx_count"))
        
        return address_stats
    
    def get_time_pattern_analysis(self):
        """
        시간별 트랜잭션 패턴 분석
        """
        df = self.get_transfers_df()
        
        # timestamp 컬럼이 있다고 가정
        if "timestamp" in df.columns:
            # 타임스탬프 시간 추출
            from pyspark.sql.functions import hour, from_unixtime
            
            # 타임스탬프가 유닉스 타임스탬프라고 가정
            df = df.withColumn("hour", hour(from_unixtime(col("timestamp"))))
            
            # 시간별 트랜잭션 수 
            time_pattern = df.groupBy("hour").agg(
                count("tx_hash").alias("tx_count")
            ).orderBy("hour")
            
            return time_pattern
        
        return None
    
    def get_top_accounts(self, n=10):
        """
        상위 N개 계정 추출
        """
        df = self.get_transfers_df()
        
        # 송신 계정 기준
        top_senders = df.groupBy("from_address").agg(
            count("tx_hash").alias("tx_count"),
            sum("value").alias("total_value")
        ).orderBy(desc("tx_count")).limit(n)
        
        # 수신 계정 기준
        top_receivers = df.groupBy("to_address").agg(
            count("tx_hash").alias("tx_count"),
            sum("value").alias("total_value")
        ).orderBy(desc("tx_count")).limit(n)
        
        return {
            "top_senders": top_senders,
            "top_receivers": top_receivers
        }
    
    def get_data_as_pandas(self, df):
        """
        PySpark DataFrame을 Pandas DataFrame으로 변환 (API 응답용)
        """
        return df.toPandas()
    
    def close(self):
        """
        Spark 세션 종료
        """
        self.spark.stop()
