from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import length, col
from pathlib import Path
from src.spark_profiling_exception import SparkProfilingException


def read_data(spark:SparkSession=None, path:Path=None, file_format:str=None, filter_list:list=None) -> DataFrame:
    if spark is not None:
        where_clause = None
        
        if filter_list is not None:
            clause = ''
            index = 0
            for condition in filter_list:
                index = index + 1
                if index == len(filter_list):
                    clause = f'{clause} {condition}'
                else:
                    clause = f'{condition} AND'
            where_clause = f"WHERE {clause}"

        if file_format.lower() == 'csv':
            data = spark.read.csv(str(path), header='true')
        else:
            data = spark.read.format(file_format).load(str(path))

        if where_clause is not None:
            data.createOrReplaceTempView("temp_view")
            filtered_output = spark.sql(f'SELECT * FROM temp_view {where_clause}')         
            return filtered_output
        return data


def spark_data_profiling(spark:SparkSession=None, data:DataFrame=None) -> DataFrame:
        try:            
            profile_df = None
            unique = False
            view_name = 'profile'        
            total_record_count = data.count()
            if total_record_count == 0:
                raise SparkProfilingException()
            column_list = data.columns

            for column in column_list:
                if data.select(column).distinct().count() == total_record_count:
                    unique = True
                else:
                    unique = False
                
                duplicate_query = f"SELECT {column} FROM {view_name} GROUP BY {column} HAVING COUNT(*) > 1"
                column_distinct_value_count = data.select(column).distinct().count()
                column_distinct_value_percentage = float(column_distinct_value_count / total_record_count)
                column_null_count = data.select(column).filter(col(column).isNull()).count()
                column_null_percentage = float(column_null_count / total_record_count)
                column_data_type = str(data.schema[column].dataType)
                if column_data_type in ['IntegerType()', 'DoubleType()', 'FloatTYpe()']:
                    column_max_value = str(data.agg({column: 'max'}).collect()[0][0])
                    column_min_value = str(data.agg({column: 'min'}).collect()[0][0])
                else:
                    column_max_value = "None"
                    column_min_value = "None"
                
                column_name_for_string_length = f"length_of_{column}"
                data = data.withColumn(column_name_for_string_length, length(column))
                column_string_max_length = data.agg({column_name_for_string_length: "max"}).collect()[0][0]
                column_string_min_length = data.agg({column_name_for_string_length: "min"}).collect()[0][0]
                data.createOrReplaceTempView(view_name)

                column_duplicate_count = spark.sql(duplicate_query).count()
                column_duplicate_percentage = column_duplicate_count / total_record_count

                result_data = [{
                    'column_name': str(column),
                    'is_unique': str(unique),
                    'distinct_value_count': str(column_distinct_value_count),
                    'distinct_value_percentage': str(column_distinct_value_percentage),
                    'duplicate_value_count': str(column_duplicate_count),
                    'duplicate_value_percentage': str(column_duplicate_percentage),
                    'data_type': str(column_data_type),
                    'max_value': str(column_max_value),
                    'min_value': str(column_min_value),
                    'null_count': str(column_null_count),
                    'null_percentage': str(column_null_percentage),
                    'total_count': str(total_record_count),
                    'max_value_length': str(column_string_max_length),
                    'min_value_length': str(column_string_min_length)
                }]

                if profile_df is None:
                    df = spark.createDataFrame(result_data)
                    profile_df = df
                else:
                    df = spark.createDataFrame(result_data)
                    profile_df = profile_df.unionByName(df)
            return profile_df
        except Exception as e:
            print(str(e))
            return None
