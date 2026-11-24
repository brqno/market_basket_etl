import re
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType, ByteType
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Função para limpar os nomes das colunas
def limpeza_colunas():
    def clean_column_name(col_name):
        col_name = re.sub(r'[^a-zA-Z0-9]', '_', col_name)  
        col_name = re.sub(r'_{2,}', '_', col_name)  
        col_name = col_name.strip('_') 
        return col_name.lower()  
    
    return clean_column_name

# Bronze Orders
def bronze_orders(clean_column_name):

    esquema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("eval_set", StringType(), True),
    StructField("order_number", IntegerType(), True),
    StructField("order_dow", IntegerType(), True),
    StructField("order_hour_of_day", IntegerType(), True),
    StructField("days_since_prior_order", DoubleType(), True),
    ])
    
    df_bronze_orders = spark.read.csv("/Volumes/workspace/projeto_market_basket/market_basket_raw/orders.csv", 
                                    header=True, 
                                    schema=esquema, 
                                    sep=",")
    
    df_bronze_orders = df_bronze_orders.select([F.col(c).alias(clean_column_name(c)) for c in df_bronze_orders.columns])
    df_bronze_orders = df_bronze_orders.withColumn("processed_data", F.current_timestamp())
    
    (
    df_bronze_orders.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.projeto_market_basket.bronze_orders")
    )
 
    return df_bronze_orders

# Bronze Products
def bronze_products(clean_column_name):

    esquema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("aisle_id", IntegerType(), True),
    StructField("department_id", IntegerType(), True),
    ])
    
    df_bronze_products = spark.read.csv("/Volumes/workspace/projeto_market_basket/market_basket_raw/products.csv", 
                                    header=True, 
                                    schema=esquema, 
                                    sep=",")
    
    df_bronze_products = df_bronze_products.select([F.col(c).alias(clean_column_name(c)) for c in df_bronze_products.columns])
    df_bronze_products = df_bronze_products.withColumn("processed_data", F.current_timestamp())

    (
    df_bronze_products.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.projeto_market_basket.bronze_products")
    )

    return df_bronze_products

# Bronze Aisles
def bronze_aisles(clean_column_name):

    esquema = StructType([
    StructField("aisle_id", IntegerType(), True),
    StructField("aisle", StringType(), True),
    ])
    
    df_bronze_aisles = spark.read.csv("/Volumes/workspace/projeto_market_basket/market_basket_raw/products.csv", 
                                    header=True, 
                                    schema=esquema, 
                                    sep=",")
    
    df_bronze_aisles = df_bronze_aisles.select([F.col(c).alias(clean_column_name(c)) for c in df_bronze_aisles.columns])
    df_bronze_aisles = df_bronze_aisles.withColumn("processed_data", F.current_timestamp())

    (
    df_bronze_aisles.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.projeto_market_basket.bronze_aisles")
    )

    return df_bronze_aisles

# Bronze Departments
def bronze_departments(clean_column_name):

    esquema = StructType([
    StructField("department_id", IntegerType(), True),
    StructField("department", StringType(), True),
    ])
    
    df_bronze_departments = spark.read.csv("/Volumes/workspace/projeto_market_basket/market_basket_raw/products.csv", 
                                    header=True, 
                                    schema=esquema, 
                                    sep=",")
    
    df_bronze_departments = df_bronze_departments.select([F.col(c).alias(clean_column_name(c)) for c in df_bronze_departments.columns])
    df_bronze_departments = df_bronze_departments.withColumn("processed_data", F.current_timestamp())

    (
    df_bronze_departments.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.projeto_market_basket.bronze_departments")
    )

    return df_bronze_departments

# Bronze Order Products Train
def bronze_order_products_train(clean_column_name):

    esquema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("add_to_cart_order", IntegerType(), True),
    StructField("reordered", ByteType(), True),
    ])
    
    df_bronze_order_products_train = spark.read.csv("/Volumes/workspace/projeto_market_basket/market_basket_raw/products.csv", 
                                    header=True, 
                                    schema=esquema, 
                                    sep=",")
    
    df_bronze_order_products_train = df_bronze_order_products_train.select([F.col(c).alias(clean_column_name(c)) for c in df_bronze_order_products_train.columns])
    df_bronze_order_products_train = df_bronze_order_products_train.withColumn("processed_data", F.current_timestamp())

    (
    df_bronze_order_products_train.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.projeto_market_basket.bronze_order_products_train")
    )

    return df_bronze_order_products_train

# Bronze Order Products Prior
def bronze_order_products_prior(clean_column_name):

    esquema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("add_to_cart_order", IntegerType(), True),
    StructField("reordered", ByteType(), True),
    ])
    
    df_bronze_order_products_prior = spark.read.csv("/Volumes/workspace/projeto_market_basket/market_basket_raw/products.csv", 
                                    header=True, 
                                    schema=esquema, 
                                    sep=",")
    
    df_bronze_order_products_prior = df_bronze_order_products_prior.select([F.col(c).alias(clean_column_name(c)) for c in df_bronze_order_products_prior.columns])
    df_bronze_order_products_prior = df_bronze_order_products_prior.withColumn("processed_data", F.current_timestamp())

    (
    df_bronze_order_products_prior.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.projeto_market_basket.bronze_order_products_prior")
    )

    return df_bronze_order_products_prior