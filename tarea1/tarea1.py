from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType, FloatType, StructField,
                               StructType, TimestampType)

spark = SparkSession.builder.appName("Read Transactions").getOrCreate()


def join_dataframes(left, right, columns_left, columns_right):
    csv_schema = StructType([StructField('customer_id', IntegerType()),
                         StructField('amount', FloatType()),
                         StructField('purchased_at', TimestampType()),
                         ])
    dataframe = spark.read.csv("transactions.csv",
                            schema=csv_schema,
                            header=True)

    dataframe.show()

    return dataframe

join_dataframes(1,1,1,1)
