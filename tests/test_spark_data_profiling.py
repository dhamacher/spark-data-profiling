import unittest
import pyspark
from pathlib import Path
import os
from pyspark.sql import SparkSession, SQLContext, DataFrame
from pyspark.sql.functions import col, length
import pandas as pd

import src.spark_data_profiling as sdp
from src.spark_profiling_exception import SparkProfilingException

TARGET_FOLDER = 'spark-data-profiling'


def get_path():
    parent_folder = Path(os.path.abspath(__file__)).resolve().parent
    target_folder = TARGET_FOLDER
    if parent_folder.is_dir():
        for parent in parent_folder.parents:
            if parent.name == target_folder:
                return parent
    return None


def get_pandas_dataframe() -> pd.DataFrame:    
    # Create a DataFrame with your data
    data = {
        "first_name": ["John", "Jane", "Michael", "Emily", "David", "Sarah", "Christopher", "Jessica", "Matthew", "Jennifer",
                    "William", "Elizabeth", "Daniel", "Linda", "Robert", "Karen", "James", "Susan", "Charles", "Lisa"],
        "last_name": ["Doe", "Smith", "Johnson", "Williams", "Brown", "Davis", "Miller", "Moore", "Wilson", "Taylor",
                    "Anderson", "Jackson", "Harris", "White", "Martin", "Thompson", "Garcia", "Martinez", "Robinson", "Clark"],
        "address": ["123 Main St", "456 Elm St", "789 Oak St", "101 Maple Ave", "222 Pine St", "333 Birch Dr", "444 Cedar Rd",
                    "555 Redwood Ln", "666 Spruce Ct", "777 Hemlock Pl", "888 Fir Way", "999 Laurel Rd", "111 Willow Dr",
                    "222 Oakwood Ave", "333 Cedar Rd", "444 Elmwood Dr", "555 Pine Ln", "666 Birch Ct", "777 Maple Pl",
                    "888 Redwood Rd"],
        "zip_code": [12345, 67890, 23456, 78901, 34567, 45678, 56789, 67890, 78901, 89012, 90123, 11234, 12345, 23456, 34567,
                    45678, 56789, 67890, 78901, 89012]
    }

    df = pd.DataFrame(data)
    return df


def create_parquet_file():
    try:
        df = get_pandas_dataframe()
        file_path = Path(get_path() / 'tests' / 'sample_data' / 'sample_parquet.parquet')
        df.to_parquet(str(file_path), index=False)
        return True
    except Exception as e:
        print(str(e))
    return False


def create_spark_dataframe(spark=None) -> DataFrame:
    df = get_pandas_dataframe()
    spark_frame = spark.createDataFrame(df)
    return spark_frame
    

class TestSpark(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        try:
            cls.spark = SparkSession.builder.getOrCreate()
            cls.sqlcontext = SQLContext(cls.spark)
            cls.is_parquet_created = create_parquet_file()
        except Exception as e:
            cls.spark = None
            cls.sqlcontext = None
            print(str(e))


    def test_read_csv_data(self):
        if self.spark is not None:
            spark = self.spark
            path = get_path()  
            filter_list = ["first_name = 'John'", "last_name = 'Doe'"]          
            file_path = Path(path / 'tests' / 'sample_data' / 'sample.csv')
            data = sdp.read_data(spark=self.spark, path=file_path, file_format='csv')
            self.assertEqual(data.count(), 20)


    def test_read_parquet_data(self):
        if self.spark is not None and self.is_parquet_created:            
            path = Path(get_path() / 'tests' / 'sample_data' / 'sample_parquet.parquet')  
            filter_list = ["first_name = 'John'", "last_name = 'Doe'"]
            data = sdp.read_data(spark=self.spark, path=path, file_format='parquet')
            self.assertEqual(data.count(), 20)


    def test_spark_data_profiling(self):
        try:            
            path = Path(get_path() / 'tests' / 'sample_data' / 'sample_parquet.parquet')
            data = sdp.read_data(spark=self.spark, path=path, file_format='parquet')
            result = sdp.spark_data_profiling(spark=self.spark, data=data)
            print()
            self.assertNotEqual(result, None)
        except Exception as e:
            print(str(e))



if __name__ == '__main__':
    unittest.main()
