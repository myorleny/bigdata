from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType, FloatType, StructField,
                               StructType, TimestampType, StringType, DateType)
from pyspark.sql.functions import col, date_format, udf, rank, lit                              
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Viajes").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

def obtener_total_ingresos_por_codigo_postal_destino (viajes_didier_df):
    
    viajes_didier_df = viajes_didier_df.withColumn("Ingreso_por_Viaje",col('kilometros')*col('precio_kilometro'))
    viajes_didier_df.show()

    total_ingresos_por_codigo_postal_destino_df = viajes_didier_df.groupBy("codigo_postal_destino").sum("Ingreso_por_Viaje")

    total_ingresos_por_codigo_postal_destino_df = total_ingresos_por_codigo_postal_destino_df.withColumn("Origen_Destino",lit("Destino"))

    total_ingresos_por_codigo_postal_destino_df = total_ingresos_por_codigo_postal_destino_df.select(
        col('codigo_postal_destino').alias('Codigo_Postal'),
        col('Origen_Destino'),
        col('sum(Ingreso_por_Viaje)').alias('Cantidad_total_ingresos'))

    total_ingresos_por_codigo_postal_destino_df = total_ingresos_por_codigo_postal_destino_df.orderBy(col('Codigo_Postal').asc())

    return total_ingresos_por_codigo_postal_destino_df   

def unir_dataframes_total_ingresos_por_codigo_postal_origen_destino(total_ingresos_por_codigo_postal_origen_df, total_ingresos_por_codigo_postal_destino_df):
    
    total_ingresos_por_codigo_postal_df = total_ingresos_por_codigo_postal_origen_df.union(total_ingresos_por_codigo_postal_destino_df)

    return total_ingresos_por_codigo_postal_df