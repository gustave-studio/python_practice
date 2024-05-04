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

# P-011: 顧客データフレーム（df_customer）から顧客ID（customer_id）の末尾が1のものだけ全項目抽出し、10件だけ表示せよ。
print('# P-011 ----------')
df_store.filter(F.col("store_cd").rlike('.*1$')).show(10)

# P-012: 店舗データフレーム（df_store）から横浜市の店舗だけ全項目表示せよ。
print('# P-012 ----------')
df_store.filter(F.col("address").rlike('.*横浜市.*')).show(10)

# P-013: 顧客データフレーム（df_customer）から、ステータスコード（status_cd）の先頭がアルファベットのA〜Fで始まるデータを全項目抽出し、10件だけ表示せよ。
print('# P-013 ----------')
df_customer.filter(F.col("status_cd").rlike('^[A-F]')).show(10)

# P-014: 顧客データフレーム（df_customer）から、ステータスコード（status_cd）の末尾が数字の1〜9で終わるデータを全項目抽出し、10件だけ表示せよ。
print('# P-014 ----------')
df_customer.filter(F.col("status_cd").rlike('[1-9]$')).show(10)

# P-015: 顧客データフレーム（df_customer）から、ステータスコード（status_cd）の先頭がアルファベットのA〜Fで始まり、
#        末尾が数字の1〜9で終わるデータを全項目抽出し、10件だけ表示せよ。
print('# P-015 ----------')
df_customer.filter(F.col("status_cd").rlike('^[A-F].*[1-9]$')).show(10)

# P-016: 店舗データフレーム（df_store）から、電話番号（tel_no）が3桁-3桁-4桁のデータを全項目表示せよ。
print('# P-016 ----------')
df_store.filter(F.col("tel_no").rlike('[0-9]{3}-[0-9]{3}-[0-9]{4}')).show(10)

# P-17: 顧客データフレーム（df_customer）を生年月日（birth_day）で高齢順にソートし、先頭10件を全項目表示せよ。
print('# P-017 ----------')
df_customer.orderBy(F.col('birth_day')).show(10)

#P-18: 顧客データフレーム（df_customer）を生年月日（birth_day）で若い順にソートし、先頭10件を全項目表示せよ。
print('# P-018 ----------')
df_customer.orderBy(F.col('birth_day').desc()).show(10)

#P-19: レシート明細データフレーム（df_receipt）に対し、1件あたりの売上金額（amount）が高い順にランクを付与し、先頭10件を抽出せよ。
#      項目は顧客ID（customer_id）、売上金額（amount）、付与したランクを表示させること。なお、売上金額（amount）が等しい場合は同一順位を付与するものとする。
print('# P-019 ----------')
df_receipt.withColumn("rank", F.rank().over(Window.orderBy(F.col("amount").desc())))\
          .select(["customer_id", "amount", "rank"]).show(10)

#P-020: レシート明細データフレーム（df_receipt）に対し、1件あたりの売上金額（amount）が高い順にランクを付与し、先頭10件を抽出せよ。
#       項目は顧客ID（customer_id）、売上金額（amount）、付与したランクを表示させること。なお、売上金額（amount）が等しい場合でも別順位を付与すること。
print('# P-020 ----------')
df_receipt.withColumn("row_number", F.row_number().over(Window.orderBy(F.col("amount").desc())))\
          .select(["customer_id", "amount", "row_number"]).show(10)
