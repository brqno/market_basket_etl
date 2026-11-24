import importlib.util
import logging
import sys
import types
from collections import Counter
from pathlib import Path

import pytest


class FakeCondition:
	def __init__(self, predicate):
		self.predicate = predicate

	def __or__(self, other):
		return FakeCondition(
			lambda row: self.predicate(row) or other.predicate(row)
		)

	def __call__(self, row):
		return self.predicate(row)


class FakeColumn:
	def __init__(self, name):
		self.name = name

	def isNull(self):
		return FakeCondition(lambda row: row.get(self.name) is None)

	def __gt__(self, value):
		return FakeCondition(lambda row: row.get(self.name, 0) > value)


class FakeDataFrame:
	def __init__(self, rows, columns=None):
		self.rows = rows
		self.columns = columns or list(rows[0].keys()) if rows else columns or []

	def filter(self, condition):
		return FakeDataFrame(
			[row for row in self.rows if condition(row)], self.columns
		)

	def limit(self, amount):
		return FakeDataFrame(self.rows[:amount], self.columns)

	def count(self):
		return len(self.rows)

	def groupBy(self, *columns):
		return FakeGroupedData(self.rows, columns)


class FakeGroupedData:
	def __init__(self, rows, columns):
		self.rows = rows
		self.columns = columns

	def count(self):
		counts = Counter(tuple(row[column] for column in self.columns) for row in self.rows)
		rows = [dict(zip(self.columns, key), count=count) for key, count in counts.items()]
		return FakeDataFrame(rows, [*self.columns, "count"])


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
		col=FakeColumn,
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


def test_data_quality_accepts_valid_primary_keys(etl_module):
	frame = FakeDataFrame([{"order_id": 1}, {"order_id": 2}])

	result = etl_module.validate_data_quality(
		frame, "orders", required_columns=("order_id",), primary_key=("order_id",)
	)

	assert result is frame


@pytest.mark.parametrize(
	("frame", "error_message"),
	[
		(FakeDataFrame([{"other_id": 1}]), "colunas obrigatorias ausentes"),
		(FakeDataFrame([{"order_id": None}]), "valores nulos"),
		(
			FakeDataFrame([{"order_id": 1}, {"order_id": 1}]),
			"duplicatas",
		),
	],
)
def test_data_quality_rejects_invalid_primary_keys(
	etl_module, frame, error_message
):
	with pytest.raises(ValueError, match=error_message):
		etl_module.validate_data_quality(
			frame,
			"orders",
			required_columns=("order_id",),
			primary_key=("order_id",),
		)


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
