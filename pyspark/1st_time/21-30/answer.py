# pipでオリジナルの解答に必要なライブラリーをインポート
import os
import pandas as pd
import numpy as np
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import math
from sklearn import preprocessing
from sklearn.model_selection import train_test_split
from imblearn.under_sampling import RandomUnderSampler

# Spark SQL用
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Spark DataFrame用
import pyspark.pandas as ps


from pyspark.sql import SparkSession 
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("IAB") \
    .getOrCreate()

# CSV読み込み
df_category = spark.read\
              .format("csv")\
              .options(header="true", inferSchema="true")\
              .load('pyspark/100knocks-preprocess/docker/work/data/category.csv')

df_customer = spark.read\
              .format("csv")\
              .options(header="true", inferSchema="true")\
              .load('pyspark/100knocks-preprocess/docker/work/data/customer.csv')

df_geocode = spark.read\
              .format("csv")\
              .options(header="true", inferSchema="true")\
              .load('pyspark/100knocks-preprocess/docker/work/data/geocode.csv')

df_product = spark.read\
              .format("csv")\
              .options(header="true", inferSchema="true")\
              .load('pyspark/100knocks-preprocess/docker/work/data/product.csv')

df_receipt = spark.read\
              .format("csv")\
              .options(header="true", inferSchema="true")\
              .load('pyspark/100knocks-preprocess/docker/work/data/receipt.csv')

df_store = spark.read\
              .format("csv")\
              .options(header="true", inferSchema="true")\
              .load('pyspark/100knocks-preprocess/docker/work/data/store.csv')

# P-021: レシート明細データフレーム（df_receipt）に対し、件数をカウントせよ。
print('# P-021 ----------')
print(df_receipt.count())

# P-022: レシート明細データフレーム（df_receipt）の顧客ID（customer_id）に対し、ユニーク件数をカウントせよ。
print('# P-022 ----------')
print(df_receipt.select("customer_id").distinct().count())

# P-023: レシート明細データフレーム（df_receipt）に対し、店舗コード（store_cd）ごとに売上金額（amount）と売上数量（quantity）を合計せよ。
print('# P-023 ----------')
df_receipt.groupBy("store_cd").sum("amount", "quantity")\
          .withColumnRenamed("sum(amount)", "amount")\
          .withColumnRenamed("sum(quantity)", "quantity")\
          .show()

# P-024: レシート明細データフレーム（df_receipt）に対し、顧客ID（customer_id）ごとに最も新しい売上日（sales_ymd）を求め、10件表示せよ。
print('# P-024 ----------')
df_receipt.groupBy("customer_id").max("sales_ymd").show(10)

# P-025: レシート明細データフレーム（df_receipt）に対し、顧客ID（customer_id）ごとに最も古い売上日（sales_ymd）を求め、10件表示せよ。
print('# P-025 ----------')
df_receipt.groupBy("customer_id").min("sales_ymd").show(10)

# P-026: レシート明細データフレーム（df_receipt）に対し、顧客ID（customer_id）ごとに最も新しい売上日（sales_ymd）と古い売上日を求め、
# 両者が異なるデータを10件表示せよ。
print('# P-026 ----------')
df_receipt_min = df_receipt.groupBy("customer_id").agg({"sales_ymd":"min"})
df_receipt_max = df_receipt.groupBy("customer_id").agg({"sales_ymd":"max"})
df_receipt_merge = df_receipt_min.join(df_receipt_max, df_receipt_min["customer_id"]==df_receipt_max["customer_id"])
df_receipt_merge.filter(F.col("min(sales_ymd)") != F.col("max(sales_ymd)")).show()

# P-027: レシート明細データフレーム（df_receipt）に対し、店舗コード（store_cd）ごとに売上金額（amount）の平均を計算し、降順でTOP5を表示せよ。
print('# P-027 ----------')
df_receipt.groupBy("store_cd").avg("amount").orderBy(F.col("avg(amount)").desc()).show(5)

# P-028: レシート明細データフレーム（df_receipt）に対し、店舗コード（store_cd）ごとに売上金額（amount）の中央値を計算し、降順でTOP5を表示せよ。
print('# P-028 ----------')
import numpy as np
import scipy as sp

@F.udf(DoubleType())
def udf_median(values_list):
    med = np.median(values_list)
    return float(med)

@F.udf(DoubleType())
def udf_mode(values_list):
    unique_values, counts = np.unique(values_list, return_counts=True)
    max_count_index = np.argmax(counts)
    mode_count = counts[max_count_index]
    return float(mode_count)


df_receipt.groupBy("store_cd").agg(udf_median(F.collect_list('amount')).alias('median_amount')).orderBy(F.col("median_amount").desc()).show(5)

# P-029: レシート明細データフレーム（df_receipt）に対し、店舗コード（store_cd）ごとに商品コード（product_cd）の最頻値を求めよ。
print('# P-029 ----------')
df_receipt.groupBy("store_cd").agg(udf_mode(F.collect_list('product_cd')).alias('mode_product_cd')).orderBy(F.col("mode_product_cd").desc()).show()

# P-030: レシート明細データフレーム（df_receipt）に対し、店舗コード（store_cd）ごとに売上金額（amount）の標本分散を計算し、降順でTOP5を表示せよ。
print('# P-030 ----------')
df_receipt.groupBy("store_cd").agg(F.variance(F.col("amount")).alias("variance")).orderBy(F.col("variance").desc()).show(5)
