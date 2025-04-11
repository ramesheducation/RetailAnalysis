import pytest
from lib.DataReader import read_customers, read_orders
from lib.DataManipulation import filter_closed_orders, count_orders_state, filter_orders_generic
from lib.ConfigReader import get_app_config
from conftest import spark

@pytest.mark.skip(reason="Skipping test for now")
def test_read_customers_df(spark):
    # Create a Spark session
    #spark = get_pyspark_session("LOCAL")
    customers_count = read_customers(spark,"LOCAL").count()
    assert customers_count == 12435

@pytest.mark.skip(reason="Skipping test for now")
def test_read_orders_df(spark):
    #spark = get_pyspark_session("LOCAL") 
    orders_count = read_orders(spark, "LOCAL").count()
    assert orders_count == 68884
    
@pytest.mark.skip(reason="Skipping transformation test temporarily")
def test_filter_closed_orders_df(spark):
    #spark = get_pyspark_session("LOCAL") 
    orders = read_orders(spark, "LOCAL")
    filtered_count = filter_closed_orders(orders).count()
    assert filtered_count == 7556
    
@pytest.mark.skip(reason="Skipping join test temporarily")
def test_read_app_config():
    config = get_app_config("LOCAL")
    assert config["orders.file.path"] == "data/orders.csv"    
    
@pytest.mark.skip(reason="Skipping transformation test temporarily")
def test_count_orders_state(spark, expected_results):
    customers_df = read_customers(spark, "LOCAL")
    actual_results = count_orders_state(customers_df)
    assert actual_results.collect() == expected_results.collect()
    
@pytest.mark.skip(reason="Skipping transformation test temporarily")
def test_check_Closed_count_df(spark):
    orders_df = read_orders(spark, "LOCAL")
    filtered_count = filter_orders_generic(orders_df,"CLOSED").count()
    assert filtered_count == 7556    

@pytest.mark.skip(reason="Skipping transformation test temporarily")
def test_check_PendingPayment_count_df(spark):
    orders_df = read_orders(spark, "LOCAL")
    filtered_count = filter_orders_generic(orders_df,"PENDING_PAYMENT").count()
    assert filtered_count == 15030 
    
@pytest.mark.skip(reason="Skipping transformation test temporarily")
def test_check_Completed_count_df(spark):
    orders_df = read_orders(spark, "LOCAL")
    filtered_count = filter_orders_generic(orders_df,"COMPLETE").count()
    assert filtered_count == 22900    

@pytest.mark.parametrize(
        "status,count",
        [
         ("CLOSED", 7556),
         ("PENDING_PAYMENT", 15030),
         ("COMPLETE", 22900)
        ]
)

@pytest.mark.parametrizetest
def test_check_count(spark,status,count):
    orders_df = read_orders(spark, "LOCAL")
    filtered_count = filter_orders_generic(orders_df,status).count()
    assert filtered_count == count     
    