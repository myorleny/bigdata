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
from .tarea2_funciones import unir_dataframes_metricas

#Pruebas para la función obtener_total_viajes_por_codigo_postal_origen

def test_total_viajes_por_codigo_postal_origen_1_viaje_por_codigo_postal(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, 5.0, 600),
                        (10000, 20302, 20105, 38.5, 290),
                        (10001, 11504, 20101, 20.0, 800)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_total_viajes_por_codigo_postal_origen(viajes_didier_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (11504, 'Origen', 1),
            (20101, 'Origen', 1),
            (20302, 'Origen', 1),
        ],
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Viajes'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect() 

def test_total_viajes_por_codigo_postal_origen_varios_viajes_por_codigo_postal(spark_session):


    viajes_didier_data = [(10000, 20101, 20105, 5.0, 600),
                        (10000, 20302, 20105, 38.5, 290),
                        (10001, 11504, 20101, 20.0, 800),
                        (10005, 20101, 60101, 100.8, 650),
                        (10200, 20101, 11501, 22.3, 490),
                        (10198, 11504, 11501, 2.0, 500)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_total_viajes_por_codigo_postal_origen(viajes_didier_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (11504, 'Origen', 2),
            (20101, 'Origen', 3),
            (20302, 'Origen', 1),
        ],
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Viajes'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()   

def test_total_viajes_por_codigo_postal_origen_mismo_viaje_varias_veces(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, 5.0, 600),
                        (10000, 20101, 20105, 5.0, 600),
                        (10000, 20101, 20105, 5.0, 600)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_total_viajes_por_codigo_postal_origen(viajes_didier_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (20101, 'Origen', 3),
        ],
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Viajes'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()    

def test_total_viajes_por_codigo_postal_origen_kilometros_negativos_cero_null(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, -5.0, 600),
                        (10000, 20302, 20105, 38.5, 290),
                        (10001, 11504, 20101, None, 800),
                        (10005, 20101, 60101, 100.8, 650),
                        (10200, 20101, 11501, 22.3, 490),
                        (10198, 11504, 11501, 0.0, 500)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_total_viajes_por_codigo_postal_origen(viajes_didier_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (20101, 'Origen', 2),
            (20302, 'Origen', 1),
        ],
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Viajes'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()    

def test_total_viajes_por_codigo_postal_origen_precioKm_negativo_cero_null(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, 5.0, -600),
                        (10000, 20302, 20105, 38.5, 290),
                        (10001, 11504, 20101, 35.0, None),
                        (10005, 20101, 60101, 100.8, 650),
                        (10200, 20101, 11501, 22.3, 490),
                        (10198, 11504, 11501, 2.0, 0)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_total_viajes_por_codigo_postal_origen(viajes_didier_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (20101, 'Origen', 2),
            (20302, 'Origen', 1),
        ],
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Viajes'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()   

def test_total_viajes_por_codigo_postal_origen_invalido(spark_session):
    viajes_didier_data = [(10000,20101, 20105, 5.0, 600),
                        (10005, None, 60101, 100.8, 650),
                        (10198,None, 11501, 2.0, 0)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_total_viajes_por_codigo_postal_origen(viajes_didier_ds)
    
    esperado_ds = spark_session.createDataFrame(
        [
            (20101, 'Origen', 1),
        ],
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Viajes'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()

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

def test_total_viajes_por_codigo_postal_destino_invalido(spark_session):
    viajes_didier_data = [(10000,20105 , 20101, 5.0, 600),
                        (10005, 60101, None, 100.8, 650),
                        (10198,11501 , None, 2.0, 0)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_total_viajes_por_codigo_postal_destino(viajes_didier_ds)
    
    esperado_ds = spark_session.createDataFrame(
        [
            (20101, 'Destino', 1), 
        ],
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Viajes'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()  

# Pruebas para la función unir_dataframes 
def test_unir_dataframes_total_viajes_por_codigo_postal_origen_destino_verifica_datos(spark_session):
    total_viajes_por_codigo_postal_origen_data = [(11504, 'Origen', 2),
                                                (20101, 'Origen', 3),
                                                (20302, 'Origen', 1)]

    total_viajes_por_codigo_postal_origen_ds = spark_session.createDataFrame(total_viajes_por_codigo_postal_origen_data,
                                              ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Viajes'])
                                                
    total_viajes_por_codigo_postal_destino_data = [ (11501, 'Destino', 3),
                                                    (20101, 'Destino', 3)]

    total_viajes_por_codigo_postal_destino_ds = spark_session.createDataFrame(total_viajes_por_codigo_postal_destino_data,
                                              ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Viajes'])
                                                
    
    actual_ds = unir_dataframes(total_viajes_por_codigo_postal_origen_ds, total_viajes_por_codigo_postal_destino_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (11504, 'Origen', 2),
            (20101, 'Origen', 3),
            (20302, 'Origen', 1),
            (11501, 'Destino', 3),
            (20101, 'Destino', 3),
        ],
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Viajes'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()  

def test_unir_dataframes_total_viajes_por_codigo_postal_origen_destino_verifica_cantidad_registros(spark_session):
    total_viajes_por_codigo_postal_origen_data = [(11504, 'Origen', 2),
                                                (20101, 'Origen', 3),
                                                (20302, 'Origen', 1)]

    total_viajes_por_codigo_postal_origen_ds = spark_session.createDataFrame(total_viajes_por_codigo_postal_origen_data,
                                              ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Viajes'])
                                                
    total_viajes_por_codigo_postal_destino_data = [ (11501, 'Destino', 3),
                                                    (20101, 'Destino', 3)]

    total_viajes_por_codigo_postal_destino_ds = spark_session.createDataFrame(total_viajes_por_codigo_postal_destino_data,
                                              ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Viajes'])
                                                
    
    actual_ds = unir_dataframes(total_viajes_por_codigo_postal_origen_ds, total_viajes_por_codigo_postal_destino_ds)
    actual = actual_ds.count()

    esperado = total_viajes_por_codigo_postal_origen_ds.count() + total_viajes_por_codigo_postal_destino_ds.count()

    assert actual == esperado                         

#Pruebas para la función obtener_total_ingresos_por_codigo_postal_origen

def test_total_ingresos_por_codigo_postal_origen_1_viaje_por_codigo_postal(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, 5.0, 600),
                        (10000, 20302, 20105, 38.5, 290),
                        (10001, 11504, 20101, 20.0, 800)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_total_ingresos_por_codigo_postal_origen(viajes_didier_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (11504, 'Origen', 16000.0),
            (20101, 'Origen', 3000.0),
            (20302, 'Origen', 11165.0),
        ],
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Ingresos'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect() 

def test_total_ingresos_por_codigo_postal_origen_varios_viajes_por_codigo_postal(spark_session):


    viajes_didier_data = [(10000, 20101, 20105, 5.0, 600),
                        (10000, 20302, 20105, 38.5, 290),
                        (10001, 11504, 20101, 20.0, 800),
                        (10005, 20101, 60101, 100.8, 650),
                        (10200, 20101, 11501, 22.3, 490),
                        (10198, 11504, 11501, 2.0, 500)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_total_ingresos_por_codigo_postal_origen(viajes_didier_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (11504, 'Origen', 17000.0),
            (20101, 'Origen', 79447.0),
            (20302, 'Origen', 11165.0),
        ],
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Ingresos'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()   

def test_total_ingresos_por_codigo_postal_origen_mismo_viaje_varias_veces(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, 5.0, 600),
                        (10000, 20101, 20105, 5.0, 600),
                        (10000, 20101, 20105, 5.0, 600)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_total_ingresos_por_codigo_postal_origen(viajes_didier_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (20101, 'Origen', 9000.0),
        ],
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Ingresos'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()    

def test_total_ingresos_por_codigo_postal_origen_kilometros_negativos_cero_null(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, -5.0, 600),
                        (10000, 20302, 20105, 38.5, 290),
                        (10001, 11504, 20101, None, 800),
                        (10005, 20101, 60101, 100.8, 650),
                        (10200, 20101, 11501, 22.3, 490),
                        (10198, 11504, 11501, 0.0, 500)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_total_ingresos_por_codigo_postal_origen(viajes_didier_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (20101, 'Origen', 76447.0),
            (20302, 'Origen', 11165.0),
        ],
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Ingresos'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()    

def test_total_ingresos_por_codigo_postal_origen_precioKm_negativo_cero_null(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, 5.0, -600),
                        (10000, 20302, 20105, 38.5, 290),
                        (10001, 11504, 20101, 35.0, None),
                        (10005, 20101, 60101, 100.8, 650),
                        (10200, 20101, 11501, 22.3, 490),
                        (10198, 11504, 11501, 2.0, 0)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_total_ingresos_por_codigo_postal_origen(viajes_didier_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (20101, 'Origen', 76447.0),
            (20302, 'Origen', 11165.0),
        ],
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Ingresos'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()   

def test_total_ingresos_por_codigo_postal_origen_invalido(spark_session):
    viajes_didier_data = [(10000,20101, 20105, 5.0, 600),
                        (10005, None, 60101, 100.8, 650),
                        (10198,None, 11501, 2.0, 0)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_total_ingresos_por_codigo_postal_origen(viajes_didier_ds)
    
    esperado_ds = spark_session.createDataFrame(
        [
            (20101, 'Origen', 3000.0),
        ],
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Ingresos'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()

#Pruebas para la función obtener_total_ingresos_por_codigo_postal_destino

def test_total_ingresos_por_codigo_postal_destino_1_viaje_por_codigo_postal(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, 5.0, 600),
                        (10000, 20302, 20106, 38.5, 290),
                        (10001, 11504, 20101, 20.0, 800)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_total_ingresos_por_codigo_postal_destino(viajes_didier_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (20101, 'Destino', 16000.0),
            (20105, 'Destino', 3000.0),
            (20106, 'Destino', 11165.0),
        ],
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Ingresos'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect() 

def test_total_ingresos_por_codigo_postal_destino_varios_viajes_por_codigo_postal(spark_session):


    viajes_didier_data = [(10000, 20101, 20105, 5.0, 600),
                        (10000, 20302, 20105, 38.5, 290),
                        (10001, 11504, 20101, 20.0, 800),
                        (10200, 20101, 11501, 22.3, 490),
                        (10198, 11504, 11501, 2.0, 500)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_total_ingresos_por_codigo_postal_destino(viajes_didier_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (11501, 'Destino', 11927.0),
            (20101, 'Destino', 16000.0),
            (20105, 'Destino', 14165.0),
        ],
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Ingresos'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()   

def test_total_ingresos_por_codigo_postal_destino_mismo_viaje_varias_veces(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, 5.0, 600),
                        (10000, 20101, 20105, 5.0, 600),
                        (10000, 20101, 20105, 5.0, 600)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_total_ingresos_por_codigo_postal_destino(viajes_didier_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (20105, 'Destino', 9000.0),
        ],
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Ingresos'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()    

def test_total_ingresos_por_codigo_postal_destino_kilometros_negativos_cero_null(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, -5.0, 600),
                        (10000, 20302, 20105, 38.5, 290),
                        (10001, 11504, 20101, None, 800),
                        (10005, 20101, 60101, 100.8, 650),
                        (10200, 20101, 11501, 22.3, 490),
                        (10198, 11504, 11501, 0.0, 500)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_total_ingresos_por_codigo_postal_destino(viajes_didier_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (11501, 'Destino', 10927.0),
            (20105, 'Destino', 11165.0),
            (60101, 'Destino', 65520.0),
        ],
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Ingresos'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()    

def test_total_ingresos_por_codigo_postal_destino_precioKm_negativo_cero_null(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, 5.0, -600),
                        (10000, 20302, 20105, 38.5, 290),
                        (10001, 11504, 20101, 35.0, None),
                        (10005, 20101, 60101, 100.8, 650),
                        (10200, 20101, 11501, 22.3, 490),
                        (10198, 11504, 11501, 2.0, 0)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_total_ingresos_por_codigo_postal_destino(viajes_didier_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (11501, 'Destino', 10927.0),
            (20105, 'Destino', 11165.0),
            (60101, 'Destino', 65520.0),
        ],
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Ingresos'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()   

def test_total_ingresos_por_codigo_postal_destino_invalido(spark_session):
    viajes_didier_data = [(10000,20105 , 20101, 5.0, 600),
                        (10005, 60101, None, 100.8, 650),
                        (10198,11501 , None, 2.0, 0)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_total_ingresos_por_codigo_postal_destino(viajes_didier_ds)
    
    esperado_ds = spark_session.createDataFrame(
        [
            (20101, 'Destino', 3000.0), 
        ],
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Ingresos'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()      

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

#Pruebas para la función obtener_metrica_persona_con_mas_kilometros

def test_metrica_persona_con_mas_kilometros_1_viaje_por_persona(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, 5.0, 600),
                        (10820, 20302, 20105, 38.5, 290),
                        (10011, 11504, 20101, 20.0, 800)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_metrica_persona_con_mas_kilometros(viajes_didier_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            ('persona_con_mas_kilometros', 10820),
        ],
        ['Tipo_de_Metrica', 'Valor'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()

def test_metrica_persona_con_mas_kilometros_varios_viajes_por_persona(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, 5.0, 600),
                        (10000, 20302, 20105, 38.5, 290),
                        (10000, 11504, 20101, 20.0, 800),
                        (10001, 20101, 60101, 100.8, 650),
                        (10001, 20101, 11501, 22.3, 490),
                        (10198, 11504, 11501, 2.0, 500)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_metrica_persona_con_mas_kilometros(viajes_didier_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            ('persona_con_mas_kilometros', 10001),
        ],
        ['Tipo_de_Metrica', 'Valor'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()   

def test_metrica_persona_con_mas_kilometros_personas_empatadas(spark_session):
    viajes_didier_data = [(10198, 20101, 60101, 100.8, 650),
                        (10198, 20102, 11501, 22.3, 490),
                        (10001, 20101, 60101, 100.8, 650),
                        (10001, 20102, 11501, 22.3, 490)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_metrica_persona_con_mas_kilometros(viajes_didier_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            ('persona_con_mas_kilometros', 10001),
        ],
        ['Tipo_de_Metrica', 'Valor'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()     

#Pruebas para la función obtener_metrica_persona_con_mas_ingresos

def test_metrica_persona_con_mas_ingresos_1_viaje_por_persona(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, 5.0, 600),
                        (10820, 20302, 20105, 38.5, 290),
                        (10011, 11504, 20101, 20.0, 800)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_metrica_persona_con_mas_ingresos(viajes_didier_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            ('persona_con_mas_ingresos', 10011),
        ],
        ['Tipo_de_Metrica', 'Valor'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()

def test_metrica_persona_con_mas_ingresos_varios_viajes_por_persona(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, 5.0, 600),
                        (10000, 20302, 20105, 38.5, 290),
                        (10000, 11504, 20101, 20.0, 800),
                        (10001, 20101, 60101, 100.8, 650),
                        (10001, 20101, 11501, 22.3, 490),
                        (10198, 11504, 11501, 2.0, 500)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_metrica_persona_con_mas_ingresos(viajes_didier_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            ('persona_con_mas_ingresos', 10001),
        ],
        ['Tipo_de_Metrica', 'Valor'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()   

def test_metrica_persona_con_mas_ingresos_personas_empatadas(spark_session):
    viajes_didier_data = [(10198, 20101, 60101, 100.8, 650),
                        (10198, 20102, 11501, 22.3, 490),
                        (10001, 20101, 60101, 100.8, 650),
                        (10001, 20102, 11501, 22.3, 490)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_metrica_persona_con_mas_ingresos(viajes_didier_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            ('persona_con_mas_ingresos', 10001),
        ],
        ['Tipo_de_Metrica', 'Valor'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()     

       
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

def test_metrica_percentil_negativo(spark_session):
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

    actual_ds = calcular_metrica_percentil(viajes_didier_ds,-1)

    esperado_ds = spark_session.createDataFrame(
        [
            ('percentil_0', 1000.0),
        ],
        ['Tipo_de_Metrica', 'Valor'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()           

#Pruebas para la función obtener_metrica_codigo_postal_origen_con_mas_ingresos

def test_metrica_codigo_postal_origen_con_mas_ingresos_1_viaje_por_codigo_postal(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, 5.0, 600),
                        (10820, 20302, 20105, 38.5, 290),
                        (10011, 11504, 20101, 20.0, 800)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_metrica_codigo_postal_origen_con_mas_ingresos(viajes_didier_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            ('codigo_postal_origen_con_mas_ingresos', 11504),
        ],
        ['Tipo_de_Metrica', 'Valor'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()

def test_metrica_codigo_postal_origen_con_mas_ingresos_varios_viajes_por_codigo_postal(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, 5.0, 600),
                        (10002, 20302, 20105, 38.5, 290),
                        (10003, 11504, 20101, 20.0, 800),
                        (10001, 20101, 60101, 100.8, 650),
                        (10001, 20101, 11501, 22.3, 490),
                        (10198, 11504, 11501, 2.0, 500)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_metrica_codigo_postal_origen_con_mas_ingresos(viajes_didier_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            ('codigo_postal_origen_con_mas_ingresos', 20101),
        ],
        ['Tipo_de_Metrica', 'Valor'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()   

def test_metrica_codigo_postal_origen_con_mas_ingresos_codigos_postales_empatados(spark_session):
    viajes_didier_data = [(10198, 20101, 60101, 100.8, 650),
                        (10198, 20102, 11501, 22.3, 490),
                        (10001, 20101, 60101, 100.8, 650),
                        (10001, 20102, 11501, 22.3, 490)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_metrica_codigo_postal_origen_con_mas_ingresos(viajes_didier_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            ('codigo_postal_origen_con_mas_ingresos', 20101),
        ],
        ['Tipo_de_Metrica', 'Valor'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()     

def test_metrica_codigo_postal_origen_con_mas_ingresos_registros_con_datos_invalidos(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, 0.0, 600),
                        (10002, 20302, 20105, 38.5, 290),
                        (10003, None, 20101, 20.0, 800),
                        (10001, 20101, 60101, -100.8, 650),
                        (10001, 20101, 11501, 22.3, None),
                        (10198, 11504, 11501, 2.0, 500)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_metrica_codigo_postal_origen_con_mas_ingresos(viajes_didier_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            ('codigo_postal_origen_con_mas_ingresos', 20302),
        ],
        ['Tipo_de_Metrica', 'Valor'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()  

#Pruebas para la función obtener_metrica_codigo_postal_destino_con_mas_ingresos

def test_metrica_codigo_postal_destino_con_mas_ingresos_1_viaje_por_codigo_postal(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, 5.0, 600),
                        (10820, 20302, 20106, 38.5, 290),
                        (10011, 11504, 20101, 20.0, 800)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_metrica_codigo_postal_destino_con_mas_ingresos(viajes_didier_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            ('codigo_postal_destino_con_mas_ingresos', 20101),
        ],
        ['Tipo_de_Metrica', 'Valor'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()

def test_metrica_codigo_postal_destino_con_mas_ingresos_varios_viajes_por_codigo_postal(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, 5.0, 600),
                        (10002, 20302, 20105, 38.5, 290),
                        (10003, 11504, 60101, 100.8, 800),
                        (10001, 20101, 60101, 90.8, 650),
                        (10001, 20101, 11501, 22.3, 490),
                        (10198, 11504, 11501, 2.0, 500)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_metrica_codigo_postal_destino_con_mas_ingresos(viajes_didier_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            ('codigo_postal_destino_con_mas_ingresos', 60101),
        ],
        ['Tipo_de_Metrica', 'Valor'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()   

def test_metrica_codigo_postal_destino_con_mas_ingresos_codigos_postales_empatados(spark_session):
    viajes_didier_data = [(10198, 20101, 60101, 100.8, 650),
                        (10198, 20102, 11501, 22.3, 490),
                        (10001, 20101, 60101, 100.8, 650),
                        (10001, 20102, 11501, 22.3, 490)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_metrica_codigo_postal_destino_con_mas_ingresos(viajes_didier_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            ('codigo_postal_destino_con_mas_ingresos', 60101),
        ],
        ['Tipo_de_Metrica', 'Valor'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()     

def test_metrica_codigo_postal_destino_con_mas_ingresos_registros_con_datos_invalidos(spark_session):
    viajes_didier_data = [(10000, 20101, 20105, 0.0, 600),
                        (10002, 20302, 20105, 38.5, 290),
                        (10003, 20102, None, 20.0, 800),
                        (10001, 20101, 60101, -100.8, 650),
                        (10001, 20101, 11501, 22.3, None),
                        (10198, 11504, 11501, 2.0, 500)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_metrica_codigo_postal_destino_con_mas_ingresos(viajes_didier_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            ('codigo_postal_destino_con_mas_ingresos', 20105),
        ],
        ['Tipo_de_Metrica', 'Valor'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()  

# Pruebas para la función unir_dataframes_metricas 
def test_unir_dataframes_metricas_verifica_datos(spark_session):
    metrica_persona_con_mas_kilometros_data = [('persona_con_mas_kilometros', 10001)]
    metrica_persona_con_mas_kilometros_ds = spark_session.createDataFrame(metrica_persona_con_mas_kilometros_data, 
                                            ['Tipo_de_Metrica', 'Valor'])
                                                
    metrica_persona_con_mas_ingresos_data = [('persona_con_mas_ingresos', 10002)]
    metrica_persona_con_mas_ingresos_ds = spark_session.createDataFrame(metrica_persona_con_mas_ingresos_data,
                                              ['Tipo_de_Metrica', 'Valor'])

    metrica_percentil_25_data = [('percentil_25', 125005)]
    metrica_percentil_25_ds = spark_session.createDataFrame(metrica_percentil_25_data,
                                              ['Tipo_de_Metrica', 'Valor'])  
    
    metrica_percentil_50_data = [('percentil_50', 145959)]
    metrica_percentil_50_ds = spark_session.createDataFrame(metrica_percentil_50_data,
                                              ['Tipo_de_Metrica', 'Valor'])  

    metrica_percentil_75_data = [('percentil_75', 148642)]
    metrica_percentil_75_ds = spark_session.createDataFrame(metrica_percentil_75_data,
                                              ['Tipo_de_Metrica', 'Valor'])                                                                                                                                        
                                                
    metrica_codigo_postal_origen_con_mas_ingresos_data = [('codigo_postal_origen_con_mas_ingresos', 10101)]
    metrica_codigo_postal_origen_con_mas_ingresos_ds = spark_session.createDataFrame(metrica_codigo_postal_origen_con_mas_ingresos_data,
                                              ['Tipo_de_Metrica', 'Valor'])  

    metrica_codigo_postal_destino_con_mas_ingresos_data = [('codigo_postal_destino_con_mas_ingresos', 30101)]
    metrica_codigo_postal_destino_con_mas_ingresos_ds = spark_session.createDataFrame(metrica_codigo_postal_destino_con_mas_ingresos_data,
                                              ['Tipo_de_Metrica', 'Valor'])                                                                                              
    
    actual_ds = unir_dataframes_metricas(metrica_persona_con_mas_kilometros_ds, metrica_persona_con_mas_ingresos_ds, metrica_percentil_25_ds, metrica_percentil_50_ds, metrica_percentil_75_ds, metrica_codigo_postal_origen_con_mas_ingresos_ds, metrica_codigo_postal_destino_con_mas_ingresos_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            ('persona_con_mas_kilometros', 10001),
            ('persona_con_mas_ingresos', 10002),
            ('percentil_25', 125005),
            ('percentil_50', 145959),
            ('percentil_75', 148642),
            ('codigo_postal_origen_con_mas_ingresos', 10101),
            ('codigo_postal_destino_con_mas_ingresos', 30101),
        ],
        ['Tipo_de_Metrica', 'Valor'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()  

def test_unir_dataframes_metricas_verifica_cantidad_registros(spark_session):
    metrica_persona_con_mas_kilometros_data = [('persona_con_mas_kilometros', 10001)]
    metrica_persona_con_mas_kilometros_ds = spark_session.createDataFrame(metrica_persona_con_mas_kilometros_data,
                                              ['Tipo_de_Metrica', 'Valor'])
                                                
    metrica_persona_con_mas_ingresos_data = [('persona_con_mas_ingresos', 10002)]
    metrica_persona_con_mas_ingresos_ds = spark_session.createDataFrame(metrica_persona_con_mas_ingresos_data,
                                              ['Tipo_de_Metrica', 'Valor'])

    metrica_percentil_25_data = [('percentil_25', 125005.0)]
    metrica_percentil_25_ds = spark_session.createDataFrame(metrica_percentil_25_data,
                                              ['Tipo_de_Metrica', 'Valor'])  
    
    metrica_percentil_50_data = [('percentil_50', 145959.0)]
    metrica_percentil_50_ds = spark_session.createDataFrame(metrica_percentil_50_data,
                                              ['Tipo_de_Metrica', 'Valor'])  

    metrica_percentil_75_data = [('percentil_75', 148642.0)]
    metrica_percentil_75_ds = spark_session.createDataFrame(metrica_percentil_75_data,
                                              ['Tipo_de_Metrica', 'Valor'])                                                                                                                                        
                                                
    metrica_codigo_postal_origen_con_mas_ingresos_data = [('codigo_postal_origen_con_mas_ingresos', 10101)]
    metrica_codigo_postal_origen_con_mas_ingresos_ds = spark_session.createDataFrame(metrica_codigo_postal_origen_con_mas_ingresos_data,
                                              ['Tipo_de_Metrica', 'Valor'])  

    metrica_codigo_postal_destino_con_mas_ingresos_data = [('codigo_postal_destino_con_mas_ingresos', 30101)]
    metrica_codigo_postal_destino_con_mas_ingresos_ds = spark_session.createDataFrame(metrica_codigo_postal_destino_con_mas_ingresos_data,
                                              ['Tipo_de_Metrica', 'Valor'])                                                                                              
    
    actual_ds = unir_dataframes_metricas(metrica_persona_con_mas_kilometros_ds, metrica_persona_con_mas_ingresos_ds, metrica_percentil_25_ds, metrica_percentil_50_ds, metrica_percentil_75_ds, metrica_codigo_postal_origen_con_mas_ingresos_ds, metrica_codigo_postal_destino_con_mas_ingresos_ds)
    actual = actual_ds.count()

    esperado = metrica_persona_con_mas_kilometros_ds.count() + metrica_persona_con_mas_ingresos_ds.count() + metrica_percentil_25_ds.count() + metrica_percentil_50_ds.count() + metrica_percentil_75_ds.count() + metrica_codigo_postal_origen_con_mas_ingresos_ds.count() + metrica_codigo_postal_destino_con_mas_ingresos_ds.count()

    assert actual == esperado  