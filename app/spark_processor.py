import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, desc, hour, from_unixtime, to_date, stddev, mean, variance, expr, when, lit, countDistinct, coalesce
from pyspark.sql.window import Window

class SparkProcessor:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("TransferAnalysis") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.memory", "2g") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.driver.host", "127.0.0.1") \
            .getOrCreate()
        self.data_path = os.path.join(os.path.dirname(__file__), "data")

    def get_transfers_df(self, start_date=None, end_date=None, chain_filter=None, limit=None):
        # CSV 파일 로드
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv("app/data/transfers_1613651332000_1615499822000.csv")
        
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
        address_stats = df.groupBy("fromAddress").agg(
            count("txhash").alias("tx_count"),
            sum("amount").alias("total_value"),  # value -> amount
            avg("amount").alias("avg_value")     # value -> amount
        ).orderBy(desc("tx_count"))
        return address_stats

    def get_time_pattern_analysis(self):
        df = self.get_transfers_df()
        if "timestamp" in df.columns:
            df = df.withColumn("hour", hour(from_unixtime(col("timestamp"))))
            time_pattern = df.groupBy("hour").agg(
                count("txhash").alias("tx_count")
            ).orderBy("hour")
            return time_pattern
        return None

    def get_top_accounts(self, n=10):
        df = self.get_transfers_df()
        top_senders = df.groupBy("fromAddress").agg(
            count("txhash").alias("tx_count"),
            sum("amount").alias("total_value")  # value -> amount
        ).orderBy(desc("tx_count")).limit(n)
        top_receivers = df.groupBy("toAddress").agg(
            count("txhash").alias("tx_count"),
            sum("amount").alias("total_value")  # value -> amount
        ).orderBy(desc("tx_count")).limit(n)
        return {
            "top_senders": top_senders,
            "top_receivers": top_receivers
        }

    def get_data_as_pandas(self, df):
        return df.toPandas()

    def close(self):
        self.spark.stop()
        
    # 파생변수 계산 함수 추가
    def calculate_derived_features(self, df):
        """파생변수 계산 함수"""
        from pyspark.sql.functions import stddev, mean, variance, expr, when, lit, countDistinct
        
        # 1. 기본 송수신 통계 계산
        sent_stats = df.groupBy("fromAddress").agg(
            count("txhash").alias("sent_tx_count"),
            sum("amount").alias("total_sent")  # value -> amount
        )
        
        recv_stats = df.groupBy("toAddress").agg(
            count("txhash").alias("recv_tx_count"),
            sum("amount").alias("total_received")  # value -> amount
        )
        
        # 2. 시간 패턴 분석 (배치/퀀트 특성)
        if "timestamp" in df.columns:
            # 타임스탬프를 시간으로 변환
            df = df.withColumn("hour", hour(from_unixtime(col("timestamp") / 1000)))
            df = df.withColumn("date", to_date(from_unixtime(col("timestamp") / 1000)))
            
            # 주소별 활동 시간대 엔트로피 계산 (퀀트 특성)
            time_window = Window.partitionBy("fromAddress")
            time_pattern = df.groupBy("fromAddress", "hour").count() \
                .withColumn("total", sum("count").over(time_window)) \
                .withColumn("probability", col("count") / col("total")) \
                .withColumn("entropy_component", 
                            when(col("probability") > 0, 
                                col("probability") * expr("log2(probability)")).otherwise(0)) \
                .groupBy("fromAddress") \
                .agg(sum("entropy_component").alias("hour_entropy"))
            
            # 음수 변환 (엔트로피 공식)
            time_pattern = time_pattern.withColumn("hour_entropy", -col("hour_entropy"))
            
            # 활동 일수 계산
            active_days = df.groupBy("fromAddress") \
                .agg(countDistinct("date").alias("active_days_count"))
        
        # 3. 거래 상대방 다양성 (네트워크 연결성)
        counterparty_sent = df.groupBy("fromAddress") \
            .agg(countDistinct("toAddress").alias("counterparty_count_sent"))
        
        counterparty_recv = df.groupBy("toAddress") \
            .agg(countDistinct("fromAddress").alias("counterparty_count_recv"))
        
        # 4. 외부 체인 거래 분석
        external_sent = df.filter(col("fromChain") != col("toChain")) \
            .groupBy("fromAddress") \
            .agg(
                count("txhash").alias("external_sent_tx_count"),
                sum("amount").alias("external_sent_tx_amount")  # value -> amount
            )
        
        external_recv = df.filter(col("fromChain") != col("toChain")) \
            .groupBy("toAddress") \
            .agg(
                count("txhash").alias("external_recv_tx_count"),
                sum("amount").alias("external_recv_tx_amount")  # value -> amount
            )
        
        # 5. 모든 특성 통합
        # 주소를 기준으로 모든 특성을 조인
        all_addresses = sent_stats.select("fromAddress").union(recv_stats.select("toAddress")).distinct()
        
        all_addresses_alias = all_addresses.alias("addr")
        sent_stats_alias = sent_stats.alias("sent")
        recv_stats_alias = recv_stats.alias("recv")
        time_pattern_alias = time_pattern.alias("time")
        active_days_alias = active_days.alias("days")
        counterparty_sent_alias = counterparty_sent.alias("cp_sent")
        counterparty_recv_alias = counterparty_recv.alias("cp_recv")
        external_sent_alias = external_sent.alias("ext_sent")
        external_recv_alias = external_recv.alias("ext_recv")
        
        features = all_addresses_alias \
            .join(sent_stats_alias, col("addr.fromAddress") == col("sent.fromAddress"), "left") \
            .join(recv_stats_alias, col("addr.fromAddress") == col("recv.toAddress"), "left") \
            .join(time_pattern_alias, col("addr.fromAddress") == col("time.fromAddress"), "left") \
            .join(active_days_alias, col("addr.fromAddress") == col("days.fromAddress"), "left") \
            .join(counterparty_sent_alias, col("addr.fromAddress") == col("cp_sent.fromAddress"), "left") \
            .join(counterparty_recv_alias, col("addr.fromAddress") == col("cp_recv.toAddress"), "left") \
            .join(external_sent_alias, col("addr.fromAddress") == col("ext_sent.fromAddress"), "left") \
            .join(external_recv_alias, col("addr.fromAddress") == col("ext_recv.toAddress"), "left") \
            .select(
                col("addr.fromAddress").alias("address"),
                coalesce(col("sent.sent_tx_count"), lit(0)).alias("sent_tx_count"),
                coalesce(col("sent.total_sent"), lit(0)).alias("total_sent"),
                coalesce(col("recv.recv_tx_count"), lit(0)).alias("recv_tx_count"),
                coalesce(col("recv.total_received"), lit(0)).alias("total_received"),
                coalesce(col("time.hour_entropy"), lit(0)).alias("hour_entropy"),
                coalesce(col("days.active_days_count"), lit(0)).alias("active_days_count"),
                coalesce(col("cp_sent.counterparty_count_sent"), lit(0)).alias("counterparty_count_sent"),
                coalesce(col("cp_recv.counterparty_count_recv"), lit(0)).alias("counterparty_count_recv"),
                coalesce(col("ext_sent.external_sent_tx_count"), lit(0)).alias("external_sent_tx_count"),
                coalesce(col("ext_sent.external_sent_tx_amount"), lit(0)).alias("external_sent_tx_amount"),
                coalesce(col("ext_recv.external_recv_tx_count"), lit(0)).alias("external_recv_tx_count"),
                coalesce(col("ext_recv.external_recv_tx_amount"), lit(0)).alias("external_recv_tx_amount")
            )
        
        return features

    def apply_weights_and_get_top_nodes(self, features_df, batch_quant_weight=50.0, 
                                        tx_count_weight=50.0, tx_amount_weight=50.0, top_n=10):
        """가중치 적용 및 상위 노드 선별"""
        from pyspark.sql.functions import expr
        
        # 각 특성을 정규화 (Min-Max 스케일링)
        numeric_cols = ["sent_tx_count", "total_sent", "recv_tx_count", "total_received", 
                       "hour_entropy", "active_days_count", "counterparty_count_sent", 
                       "counterparty_count_recv", "external_sent_tx_count", "external_sent_tx_amount",
                       "external_recv_tx_count", "external_recv_tx_amount"]
        
        # 정규화를 위한 통계 계산
        stats = {}
        for col_name in numeric_cols:
            min_val = features_df.agg({col_name: "min"}).collect()[0][0]
            max_val = features_df.agg({col_name: "max"}).collect()[0][0]
            if max_val == min_val:  # 모든 값이 동일한 경우
                stats[col_name] = (min_val, 1.0)  # 분모가 0이 되는 것 방지
            else:
                stats[col_name] = (min_val, max_val - min_val)
        
        # 정규화된 특성 생성
        normalized_df = features_df
        for col_name in numeric_cols:
            min_val, range_val = stats[col_name]
            normalized_df = normalized_df.withColumn(
                f"norm_{col_name}", 
                (col(col_name) - lit(min_val)) / lit(range_val)
            )
        
        # 배치/퀀트 점수 계산 (낮은 엔트로피 = 높은 규칙성 = 높은 배치/퀀트 특성)
        normalized_df = normalized_df.withColumn(
            "batch_quant_score",
            (1 - col("norm_hour_entropy")) * 0.5 + 
            col("norm_active_days_count") * 0.3 +
            (col("norm_sent_tx_count") / (col("norm_counterparty_count_sent") + 0.001)) * 0.2
        )
        
        # 거래 횟수 점수
        normalized_df = normalized_df.withColumn(
            "tx_count_score",
            col("norm_sent_tx_count") * 0.5 + col("norm_recv_tx_count") * 0.5
        )
        
        # 거래량 점수
        normalized_df = normalized_df.withColumn(
            "tx_amount_score",
            col("norm_total_sent") * 0.5 + col("norm_total_received") * 0.5
        )
        
        # 가중치 적용 및 최종 점수 계산
        normalized_df = normalized_df.withColumn(
            "final_score",
            (col("batch_quant_score") * (batch_quant_weight / 100)) +
            (col("tx_count_score") * (tx_count_weight / 100)) +
            (col("tx_amount_score") * (tx_amount_weight / 100))
        )
        
        # 신뢰도 티어 할당 (브론즈, 실버, 골드, 다이아몬드)
        normalized_df = normalized_df.withColumn(
            "tier",
            when(col("final_score") >= 0.75, "다이아몬드")
            .when(col("final_score") >= 0.5, "골드")
            .when(col("final_score") >= 0.25, "실버")
            .otherwise("브론즈")
        )
        
        # 체인 정보 추출 (주소에서 추출, 예: cosmos.gwyv -> cosmos)
        normalized_df = normalized_df.withColumn(
            "chain",
            expr("substring_index(address, '.', 1)")
        )
        
        # 상위 N개 노드 선택
        top_nodes = normalized_df.orderBy(col("final_score").desc()).limit(top_n)
        
        return top_nodes

    def get_top_weighted_nodes(self, start_date=None, end_date=None, 
                              batch_quant_weight=50.0, tx_count_weight=50.0, 
                              tx_amount_weight=50.0, top_n=10):
        """기간 및 가중치 기반 상위 노드 선별"""
        # 기간 필터링된 거래 데이터 가져오기
        df = self.get_transfers_df(start_date=start_date, end_date=end_date)
        
        # 파생변수 계산
        features_df = self.calculate_derived_features(df)
        
        # 가중치 적용 및 상위 노드 선별
        top_nodes = self.apply_weights_and_get_top_nodes(
            features_df, 
            batch_quant_weight, 
            tx_count_weight, 
            tx_amount_weight, 
            top_n
        )
        
        # 상위 노드 관련 거래 가져오기
        top_addresses = [row["address"] for row in top_nodes.collect()]
        related_txs = df.filter(
            (col("fromAddress").isin(top_addresses)) | 
            (col("toAddress").isin(top_addresses))
        ).limit(100)  # 각 노드당 약 10개 거래로 제한
        
        return top_nodes, related_txs
