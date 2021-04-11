# from pyspark.sql import SparkSession
# from pyspark.sql.types import (IntegerType, FloatType, StructField,
#                                StructType, TimestampType, StringType, DateType)
# from pyspark.sql.functions import col, date_format, udf, rank, lit                              
# from pyspark.sql.window import Window
 
# from pyspark.sql.functions import explode                               



# spark = SparkSession.builder.appName("Viajes").getOrCreate()
# spark.sparkContext.setLogLevel('WARN')

# import sys

from .tarea2_funciones import obtener_total_viajes_por_codigo_postal_origen
from .tarea2_funciones import obtener_total_viajes_por_codigo_postal_destino
from .tarea2_funciones import unir_dataframes
from .tarea2_funciones import obtener_total_ingresos_por_codigo_postal_origen
from .tarea2_funciones import obtener_total_ingresos_por_codigo_postal_destino

# Pruebas para la función unir_dataframes 
def test_unir_dataframes_total_ingresos_por_codigo_postal_origen_destino_verifica_datos(spark_session):
    total_ingresos_por_codigo_postal_origen_data = [(11504, 'Origen', 17000.0),
                                                (20101, 'Origen', 79447.0),
                                                (20302, 'Origen', 11165.0)]

    total_ingresos_por_codigo_postal_origen_ds = spark_session.createDataFrame(total_ingresos_por_codigo_postal_origen_data,
                                              ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Ingresos'])
                                                
    total_ingresos_por_codigo_postal_destino_data = [ (11501, 'Destino', 11927.0),
                                                    (20101, 'Destino', 30165.0)]

    total_ingresos_por_codigo_postal_destino_ds = spark_session.createDataFrame(total_ingresos_por_codigo_postal_destino_data,
                                              ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Ingresos'])
                                                
    
    actual_ds = unir_dataframes(total_ingresos_por_codigo_postal_origen_ds, total_ingresos_por_codigo_postal_destino_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (11504, 'Origen', 17000.0),
            (20101, 'Origen', 79447.0),
            (20302, 'Origen', 11165.0),
            (11501, 'Destino', 11927.0),
            (20101, 'Destino', 30165.0),
        ],
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Ingresos'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()  

def test_unir_dataframes_total_ingresos_por_codigo_postal_origen_destino_verifica_cantidad_registros(spark_session):
    total_ingresos_por_codigo_postal_origen_data = [(11504, 'Origen', 17000.0),
                                                (20101, 'Origen', 79447.0),
                                                (20302, 'Origen', 11165.0)]

    total_ingresos_por_codigo_postal_origen_ds = spark_session.createDataFrame(total_ingresos_por_codigo_postal_origen_data,
                                              ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Ingresos'])
                                                
    total_ingresos_por_codigo_postal_destino_data = [ (11501, 'Destino', 11927.0),
                                                    (20101, 'Destino', 30165.0)]

    total_ingresos_por_codigo_postal_destino_ds = spark_session.createDataFrame(total_ingresos_por_codigo_postal_destino_data,
                                              ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Ingresos'])
                                                
    
    actual_ds = unir_dataframes(total_ingresos_por_codigo_postal_origen_ds, total_ingresos_por_codigo_postal_destino_ds)
    actual = actual_ds.count()

    esperado = total_ingresos_por_codigo_postal_origen_ds.count() + total_ingresos_por_codigo_postal_destino_ds.count()

    assert actual == esperado   