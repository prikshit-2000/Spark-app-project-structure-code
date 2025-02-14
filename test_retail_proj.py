import pytest
from lib.DataReader import read_customers, read_orders
from lib.DataManipulation import  filter_closed_orders, filter_orders_generic, count_orders_state
from lib.ConfigReader import get_app_config

def test_read_customers(spark):
    customers_df = read_customers(spark, "LOCAL")
    assert customers_df.count() == 12435
    
    
def test_read_orders(spark):
    orders_df = read_orders(spark, "LOCAL")
    assert orders_df.count() == 68883
    
@pytest.mark.transformation()
def test_filter_closed_orders(spark):
    orders_df = read_orders(spark, "LOCAL")
    filtered_df = filter_closed_orders(orders_df)
    assert filtered_df.count() == 7556

    
def test_read_app_config():
    app_config =  get_app_config("LOCAL")
    
    assert app_config['orders.file.path'] == "data/orders.csv"
    
@pytest.mark.skip()
def test_filter_closed_orders_generic(spark):
    orders_df = read_orders(spark, "LOCAL")
    filtered_df = filter_orders_generic(orders_df, "CLOSED")
    assert filtered_df.count() == 7556
    
@pytest.mark.skip("work in process")
def test_filter_pending_payment_orders_generic(spark):
    orders_df = read_orders(spark, "LOCAL")
    filtered_df = filter_orders_generic(orders_df, "PENDING_PAYMENT")
    assert filtered_df.count() == 15030
    
@pytest.mark.skip("work in process")
def test_filter_complete_orders_generic(spark):
    orders_df = read_orders(spark, "LOCAL")
    filtered_df = filter_orders_generic(orders_df, "COMPLETE")
    assert filtered_df.count() == 22899



def test_count_orders_state(spark, expected_results):
    customers_df = read_customers(spark, "LOCAL")
    actual_results = count_orders_state(customers_df)
    expected_results.collect() == actual_results.collect() 
    
    
@pytest.mark.parametrize(
    "status,count",
    [
        ("CLOSED",7556),
        ("PENDING_PAYMENT",15030),
        ("COMPLETE", 22899)
    ]
)

@pytest.mark.latest()    
def test_filter_complete_orders_generic_df(spark,status,count):
    orders_df = read_orders(spark, "LOCAL")
    filtered_df = filter_orders_generic(orders_df, status)
    assert filtered_df.count() == count

    
    
    
    
    

    