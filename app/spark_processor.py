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
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv("app/data/transfers_1613651332000_1615499822000.csv")

        df = df.withColumn(
            "date",
            to_date(from_unixtime(col("timestamp") / 1000))
        )

        if start_date and end_date:
            df = df.filter(col("date").between(start_date, end_date))
        elif start_date:
            df = df.filter(col("date") >= start_date)
        elif end_date:
            df = df.filter(col("date") <= end_date)

        if chain_filter:
            df = df.filter(
                (col("fromChain") == chain_filter) |
                (col("toChain") == chain_filter)
            )

        if limit:
            df = df.limit(limit)
        return df

    def get_transaction_stats(self):
        df = self.get_transfers_df()
        address_stats = df.groupBy("fromAddress").agg(
            count("txhash").alias("tx_count"),
            sum("amount").alias("total_value"),
            avg("amount").alias("avg_value")
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
            sum("amount").alias("total_value")
        ).orderBy(desc("tx_count")).limit(n)

        top_receivers = df.groupBy("toAddress").agg(
            count("txhash").alias("tx_count"),
            sum("amount").alias("total_value")
        ).orderBy(desc("tx_count")).limit(n)

        return {
            "top_senders": top_senders,
            "top_receivers": top_receivers
        }

    def get_data_as_pandas(self, df):
        return df.toPandas()

    def close(self):
        self.spark.stop()

    def calculate_derived_features(self, df):
        """파생변수 계산 함수"""
        from pyspark.sql.functions import stddev, mean, variance, expr, when, lit, countDistinct

        sent_stats = df.groupBy("fromAddress").agg(
            count("txhash").alias("sent_tx_count"),
            sum("amount").alias("sent_tx_amount")
        )

        recv_stats = df.groupBy("toAddress").agg(
            count("txhash").alias("recv_tx_count"),
            sum("amount").alias("recv_tx_amount")
        )

        if "timestamp" in df.columns:
            df = df.withColumn("hour", hour(from_unixtime(col("timestamp") / 1000)))
            df = df.withColumn("date", to_date(from_unixtime(col("timestamp") / 1000)))

            time_window = Window.partitionBy("fromAddress")
            time_pattern = df.groupBy("fromAddress", "hour").count() \
                .withColumn("total", sum("count").over(time_window)) \
                .withColumn("probability", col("count") / col("total")) \
                .withColumn("entropy_component",
                            when(col("probability") > 0,
                                 col("probability") * expr("log2(probability)")).otherwise(0)) \
                .groupBy("fromAddress") \
                .agg(sum("entropy_component").alias("hour_entropy"))
            time_pattern = time_pattern.withColumn("hour_entropy", -col("hour_entropy"))

            active_days = df.groupBy("fromAddress") \
                .agg(countDistinct("date").alias("active_days_count"))

            counterparty_sent = df.groupBy("fromAddress") \
                .agg(countDistinct("toAddress").alias("counterparty_count_sent"))
            counterparty_recv = df.groupBy("toAddress") \
                .agg(countDistinct("fromAddress").alias("counterparty_count_recv"))

            external_sent = df.filter(col("fromChain") != col("toChain")) \
                .groupBy("fromAddress") \
                .agg(
                count("txhash").alias("external_sent_tx_count"),
                sum("amount").alias("external_sent_tx_amount")
            )

            external_recv = df.filter(col("fromChain") != col("toChain")) \
                .groupBy("toAddress") \
                .agg(
                count("txhash").alias("external_recv_tx_count"),
                sum("amount").alias("external_recv_tx_amount")
            )

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
                coalesce(col("sent.sent_tx_amount"), lit(0)).alias("sent_tx_amount"),
                coalesce(col("recv.recv_tx_count"), lit(0)).alias("recv_tx_count"),
                coalesce(col("recv.recv_tx_amount"), lit(0)).alias("recv_tx_amount"),
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
        else:
            return None

    def apply_weights_and_get_top_nodes(self, features_df, batch_quant_weight=50.0,
                                         tx_count_weight=50.0, tx_amount_weight=50.0, top_n=10):
        """가중치 적용 및 상위 노드 선별"""
        from pyspark.sql.functions import expr

        numeric_cols = ["sent_tx_count", "sent_tx_amount", "recv_tx_count", "recv_tx_amount",
                        "hour_entropy", "active_days_count", "counterparty_count_sent",
                        "counterparty_count_recv", "external_sent_tx_count", "external_sent_tx_amount",
                        "external_recv_tx_count", "external_recv_tx_amount"]

        stats = {}
        for col_name in numeric_cols:
            min_val = features_df.agg({col_name: "min"}).collect()[0][0]
            max_val = features_df.agg({col_name: "max"}).collect()[0][0]
            if max_val == min_val:
                stats[col_name] = (min_val, 1.0)
            else:
                stats[col_name] = (min_val, max_val - min_val)

        normalized_df = features_df

        for col_name in numeric_cols:
            min_val, range_val = stats[col_name]
            normalized_df = normalized_df.withColumn(
                f"norm_{col_name}",
                (col(col_name) - lit(min_val)) / lit(range_val)
            )

        normalized_df = normalized_df.withColumn(
            "batch_quant_score",
            (1 - col("norm_hour_entropy")) * 0.5 +
            col("norm_active_days_count") * 0.3 +
            (col("norm_sent_tx_count") / (col("norm_counterparty_count_sent") + 0.001)) * 0.2
        )

        normalized_df = normalized_df.withColumn(
            "tx_count_score",
            col("norm_sent_tx_count") * 0.5 + col("norm_recv_tx_count") * 0.5
        )

        normalized_df = normalized_df.withColumn(
            "tx_amount_score",
            col("norm_sent_tx_amount") * 0.5 + col("norm_recv_tx_amount") * 0.5
        )

        normalized_df = normalized_df.withColumn(
            "final_score",
            (col("batch_quant_score") * (batch_quant_weight / 100)) +
            (col("tx_count_score") * (tx_count_weight / 100)) +
            (col("tx_amount_score") * (tx_amount_weight / 100))
        )

        normalized_df = normalized_df.withColumn(
            "tier",
            when(col("final_score") >= 0.75, "다이아몬드")
            .when(col("final_score") >= 0.5, "골드")
            .when(col("final_score") >= 0.25, "실버")
            .otherwise("브론즈")
        )

        normalized_df = normalized_df.withColumn(
            "chain",
            expr("substring_index(address, '.', 1)")
        )

        top_nodes = normalized_df.orderBy(col("final_score").desc()).limit(top_n)
        return top_nodes

    def get_top_weighted_nodes(self, start_date=None, end_date=None,
                           batch_quant_weight=50.0, tx_count_weight=50.0,
                           tx_amount_weight=50.0, top_n=10):
        """기간 및 가중치 기반 상위 노드 선별"""
        df = self.get_transfers_df(start_date=start_date, end_date=end_date)
        
        # 파생 변수 계산
        features_df = self.calculate_derived_features(df)
        
        # 가중치 적용 및 상위 노드 선별
        top_nodes_df = self.apply_weights_and_get_top_nodes(
            features_df,
            batch_quant_weight,
            tx_count_weight,
            tx_amount_weight,
            top_n
        )
        
        # 상위 노드 주소 목록 추출
        top_addresses = [row["address"] for row in top_nodes_df.collect()]
        
        # 관련 트랜잭션 필터링 (top_k 노드들의 전후 1depth)
        related_txs_df = df.filter(
            (col("fromAddress").isin(top_addresses)) |
            (col("toAddress").isin(top_addresses))
        )
        
        # top_nodes 형식 맞추기 (필요한 필드 포함)
        top_nodes_with_tx_info = related_txs_df.select(
            "type", "txhash", "timestamp", "amount", "denom", "dpDenom",
            "fromAddress", "toAddress", "fromChain", "fromChainId", "toChain", "toChainId"
        ).distinct()
        
        # top_nodes_df와 조인하여 최종 점수와 티어 정보 추가
        # 데이터프레임에 별칭 지정
        top_nodes_df_alias = top_nodes_df.alias("tn")
        top_nodes_with_tx_info_alias = top_nodes_with_tx_info.alias("tx")
        
        top_nodes_result_df = top_nodes_with_tx_info_alias.join(
            top_nodes_df_alias,
            (col("tx.fromAddress") == col("tn.address")) |
            (col("tx.toAddress") == col("tn.address")),
            "inner"
        ).select(
            col("tx.type"),
            col("tx.txhash"),
            col("tx.timestamp"),
            col("tx.amount"),
            col("tx.denom"),
            col("tx.dpDenom"),
            col("tx.fromAddress"),
            col("tx.toAddress"),
            col("tx.fromChain"),
            col("tx.fromChainId"),
            col("tx.toChain"),
            col("tx.toChainId"),
            col("tn.address"),
            col("tn.final_score"),
            col("tn.tier"),
            col("tn.sent_tx_count"),
            col("tn.recv_tx_count"),
            col("tn.chain")
        ).distinct()
        
        # related_transactions 형식 맞추기
        related_transactions_df = related_txs_df.select(
            "type", "txhash", "timestamp", "amount", "denom", "dpDenom",
            "fromAddress", "toAddress", "fromChain", "fromChainId", "toChain", "toChainId"
        ).distinct()
        
        # top_nodes_derived_features 형식 맞추기
        # 데이터프레임에 별칭 지정
        features_df_alias = features_df.alias("f")
        
        # 여기가 중요한 부분입니다. top_nodes_df에서 address, final_score, tier 컬럼을 모두 선택합니다.
        top_nodes_df_alias2 = top_nodes_df.select("address", "final_score", "tier").alias("tn2")
        
        top_nodes_derived_features_df = features_df_alias.join(
            top_nodes_df_alias2,
            col("f.address") == col("tn2.address"),
            "inner"
        ).select(
            col("f.address"),
            col("f.sent_tx_count"),
            col("f.sent_tx_amount"),
            col("f.recv_tx_count"),
            col("f.recv_tx_amount"),
            col("f.hour_entropy"),
            col("f.active_days_count"),
            col("f.counterparty_count_sent"),
            col("f.counterparty_count_recv"),
            col("f.external_sent_tx_count"),
            col("f.external_sent_tx_amount"),
            col("f.external_recv_tx_count"),
            col("f.external_recv_tx_amount"),
            col("tn2.final_score"),
            col("tn2.tier")
        )
        
        # related_txs_derived_features 형식 맞추기
        related_txs_derived_features_df = self.calculate_derived_features(related_txs_df)
        
        # 가중치 적용 및 점수 계산
        weighted_related_txs_df = self.apply_weights_and_get_top_nodes(
            related_txs_derived_features_df,
            batch_quant_weight,
            tx_count_weight,
            tx_amount_weight,
            related_txs_derived_features_df.count()  # 모든 노드에 대해 계산
        )
        
        # 별칭 지정
        related_txs_derived_features_df_alias = related_txs_derived_features_df.alias("rtdf")
        weighted_related_txs_df_alias = weighted_related_txs_df.alias("wrtdf")
        
        # final_score와 tier 추가
        related_txs_derived_features_df = related_txs_derived_features_df_alias.join(
            weighted_related_txs_df_alias,
            col("rtdf.address") == col("wrtdf.address"),
            "inner"
        ).select(
            col("rtdf.address"),
            col("rtdf.sent_tx_count"),
            col("rtdf.sent_tx_amount"),
            col("rtdf.recv_tx_count"),
            col("rtdf.recv_tx_amount"),
            col("rtdf.hour_entropy"),
            col("rtdf.active_days_count"),
            col("rtdf.counterparty_count_sent"),
            col("rtdf.counterparty_count_recv"),
            col("rtdf.external_sent_tx_count"),
            col("rtdf.external_sent_tx_amount"),
            col("rtdf.external_recv_tx_count"),
            col("rtdf.external_recv_tx_amount"),
            col("wrtdf.final_score"),
            col("wrtdf.tier")
        )
        
        # 결과를 딕셔너리 리스트로 변환
        top_nodes = [row.asDict() for row in top_nodes_result_df.collect()]
        top_nodes_derived_features = [row.asDict() for row in top_nodes_derived_features_df.collect()]
        related_transactions = [row.asDict() for row in related_transactions_df.collect()]
        related_txs_derived_features = [row.asDict() for row in related_txs_derived_features_df.collect()]
        
        return top_nodes, top_nodes_derived_features, related_transactions, related_txs_derived_features


