import importlib.util
import logging
import sys
import types
from pathlib import Path

import pytest


class FakeWriter:
    def __init__(self):
        self.calls = []

    def format(self, value):
        self.format_value = value
        return self

    def mode(self, value):
        self.mode_value = value
        return self

    def saveAsTable(self, table_name):
        self.calls.append((self.format_value, self.mode_value, table_name))


@pytest.fixture
def etl_module(monkeypatch):
    pyspark = types.ModuleType("pyspark")
    sql = types.ModuleType("pyspark.sql")
    sql_types = types.ModuleType("pyspark.sql.types")
    sql_types.StructField = object
    sql_types.StructType = object
    sql_types.StringType = object
    sql_types.IntegerType = object
    sql_types.DoubleType = object
    sql_types.ByteType = object

    class FakeBuilder:
        @staticmethod
        def getOrCreate():
            return object()

    class FakeSparkSession:
        builder = FakeBuilder()

    sql.SparkSession = FakeSparkSession
    sql.Window = object()
    sql.functions = types.SimpleNamespace(
        col=lambda name: name,
        current_timestamp=lambda: object(),
    )
    pyspark.sql = sql

    monkeypatch.setitem(sys.modules, "pyspark", pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    monkeypatch.setitem(sys.modules, "pyspark.sql.types", sql_types)
    monkeypatch.setattr(logging, "basicConfig", lambda **_kwargs: None)

    module_path = Path(__file__).with_name("extract_data.py")
    spec = importlib.util.spec_from_file_location("extract_data_under_test", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_column_normalization_is_stable(etl_module):
    clean_column_name = etl_module.limpeza_colunas()

    normalized = clean_column_name(" Order__ID ")

    assert normalized == "order_id"
    assert clean_column_name(normalized) == normalized


def test_delta_write_is_idempotent(etl_module):
    writer = FakeWriter()
    frame = types.SimpleNamespace(write=writer)
    table_name = "workspace.projeto_market_basket.bronze_orders"

    etl_module.write_delta_table(frame, table_name)
    etl_module.write_delta_table(frame, table_name)

    assert writer.calls == [
        ("delta", "overwrite", table_name),
        ("delta", "overwrite", table_name),
    ]


def test_bronze_ingestion_runs_all_datasets_in_order(etl_module, monkeypatch):
    calls = []
    ingestion_functions = (
        "bronze_orders",
        "bronze_products",
        "bronze_aisles",
        "bronze_departments",
        "bronze_order_products_train",
        "bronze_order_products_prior",
    )

    for function_name in ingestion_functions:
        monkeypatch.setattr(
            etl_module,
            function_name,
            lambda _clean_column_name, name=function_name: calls.append(name),
        )

    etl_module.run_bronze_ingestion()

    assert calls == list(ingestion_functions)
