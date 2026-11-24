import re
import logging
from functools import reduce
from operator import or_

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType, ByteType
from pyspark.sql import SparkSession
from pathlib import Path

spark = SparkSession.builder.getOrCreate()

PROJECT_ROOT = Path(__file__).resolve().parents[1]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(PROJECT_ROOT / "pipeline_ingestao_bronze.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def validate_data_quality(df, dataset_name, required_columns, primary_key):
    missing_columns = sorted(set(required_columns).difference(df.columns))
    if missing_columns:
        raise ValueError(
            f"{dataset_name}: colunas obrigatorias ausentes: {missing_columns}"
        )

    null_key_condition = reduce(
        or_, (F.col(column).isNull() for column in required_columns)
    )
    if df.filter(null_key_condition).limit(1).count():
        raise ValueError(f"{dataset_name}: chave obrigatoria contem valores nulos")

    duplicate_keys = (
        df.groupBy(*primary_key)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicate_keys:
        raise ValueError(f"{dataset_name}: chave primaria contem duplicatas")

    logger.info("Qualidade validada para %s", dataset_name)
    return df


def write_delta_table(df, table_name):
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    return df

# Função para limpar os nomes das colunas
def limpeza_colunas():
    logger.info("Criando limpeza de nomes de colunas...")

    def clean_column_name(col_name):
        col_name = re.sub(r'[^a-zA-Z0-9]', '_', col_name)  
        col_name = re.sub(r'_{2,}', '_', col_name)  
        col_name = col_name.strip('_') 
        normalized_name = col_name.lower()
        logger.debug("Nome de coluna normalizado de %r para %r", col_name, normalized_name)
        return normalized_name
    
    return clean_column_name

# Bronze Orders
def bronze_orders(clean_column_name):
    logger.info("Começando a ingestão de: bronze_orders")

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
    validate_data_quality(
        df_bronze_orders,
        "orders",
        required_columns=("order_id", "user_id"),
        primary_key=("order_id",),
    )
    
    df_bronze_orders = df_bronze_orders.select([F.col(c).alias(clean_column_name(c)) for c in df_bronze_orders.columns])
    df_bronze_orders = df_bronze_orders.withColumn("processed_data", F.current_timestamp())
    
    write_delta_table(df_bronze_orders, "workspace.projeto_market_basket.bronze_orders")
    logger.info("Ingestão completada de: bronze_orders")
 
    return df_bronze_orders

# Bronze Products
def bronze_products(clean_column_name):
    logger.info("Começando a ingestão de: bronze_products")

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
    validate_data_quality(
        df_bronze_products,
        "products",
        required_columns=("product_id",),
        primary_key=("product_id",),
    )
    
    df_bronze_products = df_bronze_products.select([F.col(c).alias(clean_column_name(c)) for c in df_bronze_products.columns])
    df_bronze_products = df_bronze_products.withColumn("processed_data", F.current_timestamp())

    write_delta_table(df_bronze_products, "workspace.projeto_market_basket.bronze_products")
    logger.info("Ingestão completada de: bronze_products")

    return df_bronze_products

# Bronze Aisles
def bronze_aisles(clean_column_name):
    logger.info("Começando a ingestão de: bronze_aisles")

    esquema = StructType([
    StructField("aisle_id", IntegerType(), True),
    StructField("aisle", StringType(), True),
    ])
    
    df_bronze_aisles = spark.read.csv("/Volumes/workspace/projeto_market_basket/market_basket_raw/aisles.csv",
                                    header=True, 
                                    schema=esquema, 
                                    sep=",")
    validate_data_quality(
        df_bronze_aisles,
        "aisles",
        required_columns=("aisle_id",),
        primary_key=("aisle_id",),
    )
    
    df_bronze_aisles = df_bronze_aisles.select([F.col(c).alias(clean_column_name(c)) for c in df_bronze_aisles.columns])
    df_bronze_aisles = df_bronze_aisles.withColumn("processed_data", F.current_timestamp())

    write_delta_table(df_bronze_aisles, "workspace.projeto_market_basket.bronze_aisles")
    logger.info("Ingestão completada de: bronze_aisles")

    return df_bronze_aisles

# Bronze Departments
def bronze_departments(clean_column_name):
    logger.info("Começando a ingestão de: bronze_departments")

    esquema = StructType([
    StructField("department_id", IntegerType(), True),
    StructField("department", StringType(), True),
    ])
    
    df_bronze_departments = spark.read.csv("/Volumes/workspace/projeto_market_basket/market_basket_raw/departments.csv",
                                    header=True, 
                                    schema=esquema, 
                                    sep=",")
    validate_data_quality(
        df_bronze_departments,
        "departments",
        required_columns=("department_id",),
        primary_key=("department_id",),
    )
    
    df_bronze_departments = df_bronze_departments.select([F.col(c).alias(clean_column_name(c)) for c in df_bronze_departments.columns])
    df_bronze_departments = df_bronze_departments.withColumn("processed_data", F.current_timestamp())

    write_delta_table(df_bronze_departments, "workspace.projeto_market_basket.bronze_departments")
    logger.info("Ingestão completada de: bronze_departments")

    return df_bronze_departments

# Bronze Order Products Train
def bronze_order_products_train(clean_column_name):
    logger.info("Começando a ingestão de: bronze_order_products_train")

    esquema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("add_to_cart_order", IntegerType(), True),
    StructField("reordered", ByteType(), True),
    ])
    
    df_bronze_order_products_train = spark.read.csv("/Volumes/workspace/projeto_market_basket/market_basket_raw/order_products__train.csv",
                                    header=True, 
                                    schema=esquema, 
                                    sep=",")
    validate_data_quality(
        df_bronze_order_products_train,
        "order_products_train",
        required_columns=("order_id", "product_id"),
        primary_key=("order_id", "product_id"),
    )
    
    df_bronze_order_products_train = df_bronze_order_products_train.select([F.col(c).alias(clean_column_name(c)) for c in df_bronze_order_products_train.columns])
    df_bronze_order_products_train = df_bronze_order_products_train.withColumn("processed_data", F.current_timestamp())

    write_delta_table(
        df_bronze_order_products_train,
        "workspace.projeto_market_basket.bronze_order_products_train",
    )
    logger.info("Ingestão completada de: bronze_order_products_train")

    return df_bronze_order_products_train

# Bronze Order Products Prior
def bronze_order_products_prior(clean_column_name):
    logger.info("Começando a ingestão de: bronze_order_products_prior")

    esquema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("add_to_cart_order", IntegerType(), True),
    StructField("reordered", ByteType(), True),
    ])
    
    df_bronze_order_products_prior = spark.read.csv("/Volumes/workspace/projeto_market_basket/market_basket_raw/order_products__prior.csv",
                                    header=True, 
                                    schema=esquema, 
                                    sep=",")
    validate_data_quality(
        df_bronze_order_products_prior,
        "order_products_prior",
        required_columns=("order_id", "product_id"),
        primary_key=("order_id", "product_id"),
    )
    
    df_bronze_order_products_prior = df_bronze_order_products_prior.select([F.col(c).alias(clean_column_name(c)) for c in df_bronze_order_products_prior.columns])
    df_bronze_order_products_prior = df_bronze_order_products_prior.withColumn("processed_data", F.current_timestamp())

    write_delta_table(
        df_bronze_order_products_prior,
        "workspace.projeto_market_basket.bronze_order_products_prior",
    )
    logger.info("Ingestão completada de: bronze_order_products_prior")

    return df_bronze_order_products_prior