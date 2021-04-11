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
from .tarea2_funciones import obtener_metrica_persona_con_mas_kilometros
from .tarea2_funciones import obtener_metrica_persona_con_mas_ingresos
from .tarea2_funciones import obtener_metrica_codigo_postal_origen_con_mas_ingresos
from .tarea2_funciones import obtener_metrica_codigo_postal_destino_con_mas_ingresos
from .tarea2_funciones import calcular_metrica_percentil

#Pruebas para la función obtener_metrica_percentil

def test_metrica_percentil_25(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, 5.0, 600),
                        (10001, 20302, 20105, 38.5, 290),
                        (10002, 11504, 60101, 100.8, 800),
                        (10003, 20101, 60101, 90.8, 650)                      
                        ] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = calcular_metrica_percentil(viajes_didier_ds,25)

    esperado_ds = spark_session.createDataFrame(
        [
            ('percentil_25', 3000.0),
        ],
        ['Tipo_de_Metrica', 'Valor'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()

def test_metrica_percentil_25_con_redondeo(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, 5.0, 600),
                        (10001, 20302, 20105, 38.5, 290),
                        (10002, 11504, 60101, 100.8, 800),
                        (10003, 20101, 60101, 90.8, 650),
                        (10004, 20101, 11501, 22.3, 490),
                        (10005, 11504, 11501, 2.0, 500),
                        (10006, 20102, 11502, 22.3, 490),
                        (10007, 20103, 11503, 23.4, 500),
                        (10008, 20104, 11504, 24.5, 510),
                        (10009, 20105, 11505, 25.6, 520)
                        ] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = calcular_metrica_percentil(viajes_didier_ds,25)

    esperado_ds = spark_session.createDataFrame(
        [
            ('percentil_25', 10927.0),
        ],
        ['Tipo_de_Metrica', 'Valor'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()

def test_metrica_percentil_50(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, 5.0, 600),
                        (10001, 20302, 20105, 38.5, 290),
                        (10002, 11504, 60101, 100.8, 800),
                        (10003, 20101, 60101, 90.8, 650),
                        (10004, 20101, 11501, 22.3, 490),
                        (10005, 11504, 11501, 2.0, 500),
                        (10006, 20102, 11502, 22.3, 490),
                        (10007, 20103, 11503, 23.4, 500),
                        (10008, 20104, 11504, 24.5, 510),
                        (10009, 20105, 11505, 25.6, 520)
                        ] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = calcular_metrica_percentil(viajes_didier_ds,50)

    esperado_ds = spark_session.createDataFrame(
        [
            ('percentil_50', 11165.0),
        ],
        ['Tipo_de_Metrica', 'Valor'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()

def test_metrica_percentil_50_con_redondeo(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, 5.0, 600),
                        (10000, 20101, 20105, 5.0, 600),
                        (10001, 20302, 20105, 38.5, 290),
                        (10002, 11504, 60101, 100.8, 800),
                        (10002, 11504, 10101, 2.3, 495),
                        (10003, 20101, 60101, 90.8, 650),
                        (10004, 20101, 11501, 22.3, 490),
                        (10005, 11504, 11501, 2.0, 500),
                        (10006, 20102, 11502, 22.3, 490),
                        (10007, 20103, 11503, 23.4, 500),
                        (10008, 20104, 11504, 24.5, 510),
                        (10009, 20105, 11505, 25.6, 520),
                        (10010, 20102, 11507, 37.6, 350)
                        ] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = calcular_metrica_percentil(viajes_didier_ds,50)

    esperado_ds = spark_session.createDataFrame(
        [
            ('percentil_50', 11700.0),
        ],
        ['Tipo_de_Metrica', 'Valor'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()    

def test_metrica_percentil_75(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, 5.0, 600),
                        (10000, 20101, 20105, 5.0, 600),
                        (10001, 20302, 20105, 38.5, 290),
                        (10002, 11504, 60101, 100.8, 800),
                        (10002, 11504, 10101, 2.3, 495),
                        (10003, 20101, 60101, 90.8, 650),
                        (10004, 20101, 11501, 22.3, 490),
                        (10005, 11504, 11501, 2.0, 500),
                        (10006, 20102, 11502, 22.3, 490),
                        (10007, 20103, 11503, 23.4, 500),
                        (10008, 20104, 11504, 24.5, 510),
                        (10009, 20105, 11505, 25.6, 520),
                        (10010, 20105, 60105, 110.6, 800),
                        (10011, 20105, 60105, 110.6, 800)
                        ] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = calcular_metrica_percentil(viajes_didier_ds,75)

    esperado_ds = spark_session.createDataFrame(
        [
            ('percentil_75', 59020.0),
        ],
        ['Tipo_de_Metrica', 'Valor'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect() 

def test_metrica_percentil_75_con_redondeo(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, 5.0, 600),
                        (10000, 20101, 20105, 5.0, 600),
                        (10001, 20302, 20105, 38.5, 290),
                        (10002, 11504, 60101, 100.8, 800),
                        (10002, 11504, 10101, 2.3, 495),
                        (10003, 20101, 60101, 90.8, 650),
                        (10004, 20101, 11501, 22.3, 490),
                        (10005, 11504, 11501, 2.0, 500),
                        (10006, 20102, 11502, 22.3, 490),
                        (10007, 20103, 11503, 23.4, 500),
                        (10008, 20104, 11504, 24.5, 510),
                        (10009, 20105, 11505, 25.6, 520)
                        ] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = calcular_metrica_percentil(viajes_didier_ds,75)

    esperado_ds = spark_session.createDataFrame(
        [
            ('percentil_75', 13312.0),
        ],
        ['Tipo_de_Metrica', 'Valor'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()    

def test_metrica_percentil_10(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, 5.0, 600),
                        (10001, 20302, 20105, 38.5, 290),
                        (10002, 11504, 60101, 100.8, 800),
                        (10003, 20101, 60101, 90.8, 650),
                        (10004, 20101, 11501, 22.3, 490),
                        (10005, 11504, 11501, 2.0, 500),
                        (10006, 20102, 11502, 22.3, 490),
                        (10007, 20103, 11503, 23.4, 500),
                        (10008, 20104, 11504, 24.5, 510),
                        (10009, 20105, 11505, 25.6, 520)
                        ] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = calcular_metrica_percentil(viajes_didier_ds,10)

    esperado_ds = spark_session.createDataFrame(
        [
            ('percentil_10', 1000.0),
        ],
        ['Tipo_de_Metrica', 'Valor'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()

def test_metrica_percentil_90_con_redondeo(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, 5.0, 600),
                        (10000, 20101, 20105, 5.0, 600),
                        (10001, 20302, 20105, 38.5, 290),
                        (10002, 11504, 60101, 100.8, 800),
                        (10002, 11504, 10101, 2.3, 495),
                        (10003, 20101, 60101, 90.8, 650),
                        (10004, 20101, 11501, 22.3, 490),
                        (10005, 11504, 11501, 2.0, 500),
                        (10006, 20102, 11502, 22.3, 490),
                        (10007, 20103, 11503, 23.4, 500),
                        (10008, 20104, 11504, 24.5, 510),
                        (10009, 20105, 11505, 25.6, 520),
                        (10010, 20105, 60105, 110.6, 800)
                        ] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = calcular_metrica_percentil(viajes_didier_ds,90)

    esperado_ds = spark_session.createDataFrame(
        [
            ('percentil_90', 81778.5),
        ],
        ['Tipo_de_Metrica', 'Valor'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()     

def test_metrica_percentil_110(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, 5.0, 600),
                        (10000, 20101, 20105, 5.0, 600),
                        (10001, 20302, 20105, 38.5, 290),
                        (10002, 11504, 60101, 100.8, 800),
                        (10002, 11504, 10101, 2.3, 495),
                        (10003, 20101, 60101, 90.8, 650),
                        (10004, 20101, 11501, 22.3, 490),
                        (10005, 11504, 11501, 2.0, 500),
                        (10006, 20102, 11502, 22.3, 490),
                        (10007, 20103, 11503, 23.4, 500),
                        (10008, 20104, 11504, 24.5, 510),
                        (10009, 20105, 11505, 25.6, 520)
                        ] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = calcular_metrica_percentil(viajes_didier_ds,110)

    esperado_ds = spark_session.createDataFrame(
        [
            ('percentil_100', 81778.5),
        ],
        ['Tipo_de_Metrica', 'Valor'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()       

