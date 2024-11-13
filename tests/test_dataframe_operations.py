import pytest
from hey_python.dataframe_operations import test_dataframe

def test_dataframe_operations():
    operations = ["mean", "sum", "variance"]
    result_json = test_dataframe("orders", "sales", operations)
    
    assert "mean" in result_json
    assert "sum" in result_json
    assert "variance" in result_json

def test_table_insertion():
    operations = ["mean", "sum"]
    result_message = test_dataframe("orders", "sales", operations, return_table_name="statistics_results")
    
    assert result_message == "Results inserted into table"
