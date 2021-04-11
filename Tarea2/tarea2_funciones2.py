from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType, FloatType, StructField,
                               StructType, TimestampType, StringType, DateType)
from pyspark.sql.functions import col, date_format, udf, rank, lit                              
from pyspark.sql.window import Window
 
from pyspark.sql.functions import explode                               



spark = SparkSession.builder.appName("Viajes").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

import sys

#Pruebas para la función obtener_total_viajes_por_codigo_postal_destino

def test_total_viajes_por_codigo_postal_destino_1_viaje_por_codigo_postal(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, 5.0, 600),
                        (10000, 20302, 20106, 38.5, 290),
                        (10001, 11504, 20101, 20.0, 800)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_total_viajes_por_codigo_postal_destino(viajes_didier_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (20101, 'Destino', 1),
            (20105, 'Destino', 1),
            (20106, 'Destino', 1),
        ],
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Viajes'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect() 

def test_total_viajes_por_codigo_postal_destino_varios_viajes_por_codigo_postal(spark_session):


    viajes_didier_data = [(10000, 20101, 20105, 5.0, 600),
                        (10000, 20302, 20105, 38.5, 290),
                        (10001, 11504, 20101, 20.0, 800),
                        (10200, 20101, 11501, 22.3, 490),
                        (10198, 11504, 11501, 2.0, 500)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_total_viajes_por_codigo_postal_destino(viajes_didier_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (11501, 'Destino', 2),
            (20101, 'Destino', 1),
            (20105, 'Destino', 2),
        ],
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Viajes'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()   

def test_total_viajes_por_codigo_postal_destino_mismo_viaje_varias_veces(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, 5.0, 600),
                        (10000, 20101, 20105, 5.0, 600),
                        (10000, 20101, 20105, 5.0, 600)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_total_viajes_por_codigo_postal_destino(viajes_didier_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (20105, 'Destino', 3),
        ],
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Viajes'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()    

def test_total_viajes_por_codigo_postal_destino_kilometros_negativos_cero_null(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, -5.0, 600),
                        (10000, 20302, 20105, 38.5, 290),
                        (10001, 11504, 20101, None, 800),
                        (10005, 20101, 60101, 100.8, 650),
                        (10200, 20101, 11501, 22.3, 490),
                        (10198, 11504, 11501, 0.0, 500)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_total_viajes_por_codigo_postal_destino(viajes_didier_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (11501, 'Destino', 1),
            (20105, 'Destino', 1),
            (60101, 'Destino', 1),
        ],
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Viajes'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()    

def test_total_viajes_por_codigo_postal_destino_precioKm_negativo_cero_null(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, 5.0, -600),
                        (10000, 20302, 20105, 38.5, 290),
                        (10001, 11504, 20101, 35.0, None),
                        (10005, 20101, 60101, 100.8, 650),
                        (10200, 20101, 11501, 22.3, 490),
                        (10198, 11504, 11501, 2.0, 0)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_total_viajes_por_codigo_postal_destino(viajes_didier_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (11501, 'Destino', 1),
            (20105, 'Destino', 1),
            (60101, 'Destino', 1),
        ],
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Viajes'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()   

def test_total_viajes_por_codigo_postal_destino_en_blanco(spark_session):
    viajes_didier_data = [(10000,'' , 20105, 5.0, 600),
                        (10005, '', 60101, 100.8, 650),
                        (10198,'' , 11501, 2.0, 0)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_total_viajes_por_codigo_postal_destino(viajes_didier_ds)
    
    actual = actual_ds.count()

    esperado = 0

    assert actual == esperado