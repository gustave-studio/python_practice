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
              .load('data/category.csv')

df_customer = spark.read\
              .format("csv")\
              .options(header="true", inferSchema="true")\
              .load('data/customer.csv')

df_geocode = spark.read\
              .format("csv")\
              .options(header="true", inferSchema="true")\
              .load('data/geocode.csv')

df_product = spark.read\
              .format("csv")\
              .options(header="true", inferSchema="true")\
              .load('data/product.csv')

df_receipt = spark.read\
              .format("csv")\
              .options(header="true", inferSchema="true")\
              .load('data/receipt.csv')

df_store = spark.read\
              .format("csv")\
              .options(header="true", inferSchema="true")\
              .load('data/store.csv')

print('# P-001 ----------')
df_receipt.show(10, truncate=False)


# P-002: レシート明細のデータフレーム（df_receipt）から売上日（sales_ymd）、顧客ID（customer_id）、
#        商品コード（product_cd）、売上金額（amount）の順に列を指定し、10件表示させよ。
print('# P-002 ----------')
df_receipt.select(["sales_ymd", "customer_id", "product_cd", "amount"]).show(10)

# P-003: レシート明細のデータフレーム（df_receipt）から売上日（sales_ymd）、顧客ID（customer_id）、
#        商品コード（product_cd）、売上金額（amount）の順に列を指定し、10件表示させよ。
#        ただし、sales_ymdはsales_dateに項目名を変更しながら抽出すること。
print('# P-003 ----------')
df_receipt.select(["sales_ymd", "customer_id", "product_cd", "amount"])\
          .withColumnRenamed("sales_ymd", "sales_date").show(10)

# P-004: レシート明細のデータフレーム（df_receipt）から売上日（sales_ymd）、顧客ID（customer_id）、商品コード（product_cd）、
#        売上金額（amount）の順に列を指定し、以下の条件を満たすデータを抽出せよ。
#        顧客ID（customer_id）が"CS018205000001"
print('# P-004 ----------')
df_receipt.select(["sales_ymd", "customer_id", "product_cd", "amount"])\
          .filter(df_receipt["customer_id"]=="CS018205000001").show()

# P-005: レシート明細のデータフレーム（df_receipt）から売上日（sales_ymd）、顧客ID（customer_id）、商品コード（product_cd）、
#        売上金額（amount）の順に列を指定し、以下の条件を満たすデータを抽出せよ。
#        顧客ID（customer_id）が"CS018205000001"
#        売上金額（amount）が1,000以上
print('# P-005 ----------')
df_receipt.select(["sales_ymd", "customer_id", "product_cd", "amount"])\
          .filter((df_receipt["customer_id"]=="CS018205000001") & (df_receipt["amount"]>=1000)).show()

# P-006: レシート明細データフレーム「df_receipt」から売上日（sales_ymd）、顧客ID（customer_id）、商品コード（product_cd）、
#        売上数量（quantity）、売上金額（amount）の順に列を指定し、以下の条件を満たすデータを抽出せよ。
#        顧客ID（customer_id）が"CS018205000001"
#        売上金額（amount）が1,000以上または売上数量（quantity）が5以上
print('# P-006 ----------')
df_receipt.select(["sales_ymd", "customer_id", "product_cd", "quantity", "amount"])\
          .filter((df_receipt["customer_id"]=="CS018205000001") & ((df_receipt["amount"]>=1000) | (df_receipt["quantity"]>5))).show()

# P-007: レシート明細のデータフレーム（df_receipt）から売上日（sales_ymd）、顧客ID（customer_id）、商品コード（product_cd）、
#        売上金額（amount）の順に列を指定し、以下の条件を満たすデータを抽出せよ。
#        顧客ID（customer_id）が"CS018205000001"
#        売上金額（amount）が1,000以上2,000以下
print('# P-007 ----------')
df_receipt.select(["sales_ymd", "customer_id", "product_cd", "amount"])\
          .filter((F.col("customer_id")=="CS018205000001") & ((F.col("amount")>=1000) & (F.col("amount")<=2000))).show()

# P-008: レシート明細のデータフレーム（df_receipt）から売上日（sales_ymd）、顧客ID（customer_id）、商品コード（product_cd）、
#        売上金額（amount）の順に列を指定し、以下の条件を満たすデータを抽出せよ。
#        顧客ID（customer_id）が"CS018205000001"
#        商品コード（product_cd）が"P071401019"以外
print('# P-008 ----------')
df_receipt.select(["sales_ymd", "customer_id", "product_cd", "amount"])\
          .filter((F.col("customer_id")=="CS018205000001") & (F.col("product_cd")!="P071401019")).show()

# P-009: 以下の処理において、出力結果を変えずにORをANDに書き換えよ。
#        df_store.query('not(prefecture_cd == "13" | floor_area > 900)')
print('# P-009 ----------')
df_store.filter((F.col("prefecture_cd")!="13") & (F.col("floor_area")<=900)).show()

# P-010: 店舗データフレーム（df_store）から、店舗コード（store_cd）が"S14"で始まるものだけ全項目抽出し、10件だけ表示せよ。
print('# P-010 ----------')
df_store.filter(F.col("store_cd").rlike('S14.*')).show(10)