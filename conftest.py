import pytest
from lib.Utils import get_pyspark_session   

@pytest.fixture
def spark():
    "Create a fixture for the Spark session to be used in tests"
    # Create a Spark session for testing
    spark_session = get_pyspark_session("LOCAL")    
    yield spark_session
    # Teardown code (optional)  
    spark_session.stop()    
    print("Spark session stopped.")
    
@pytest.fixture     
def expected_results(spark):
    # Create a DataFrame with expected results for testing
    results_schema = "state string, count int"    
    return spark.read \
          .schema(results_schema) \
          .format("csv") \
          .load("data/test_results/state_aggregate.csv")