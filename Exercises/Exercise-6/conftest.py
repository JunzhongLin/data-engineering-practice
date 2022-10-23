import pytest
import findspark
findspark.init()
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def sparksession():
    sc = SparkSession.builder.appName("test").getOrCreate()
    yield sc
    sc.stop()