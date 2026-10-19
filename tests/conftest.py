import os
import sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

os.environ["ENTRYPOINT_SKIP_RUN"] = "1"
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "payload"))

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[1]")
         .appName("tests").config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()