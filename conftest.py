import pytest
from lib.ConfigReader import get_pyspark_config
from lib.Utils import get_spark_session

### testing l

@pytest.fixture
def spark():
    """Spark Session"""
    spark_session = get_spark_session("LOCAL")
    yield spark_session
    spark_session.stop()
    
    
@pytest.fixture
def expected_results(spark):
    "gives the expected results"
    results_schema = "state string, count int"
    
    return spark.read.format("csv") \
        .schema(results_schema) \
        .load("data/test_result/state_aggregate.csv")
        
    
    
