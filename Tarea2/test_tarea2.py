from .tarea2_funciones import obtener_total_viajes_por_codigo_postal_origen
from .tarea2_funciones import obtener_total_viajes_por_codigo_postal_destino
from .tarea2_funciones import unir_dataframes_total_viajes_por_codigo_postal_origen_destino
from .tarea2_funciones import obtener_total_ingresos_por_codigo_postal_origen
from .tarea2_funciones import obtener_total_ingresos_por_codigo_postal_destino
from .tarea2_funciones import unir_dataframes_total_ingresos_por_codigo_postal_origen_destino

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

# Pruebas para la función unir_dataframes_total_viajes_por_codigo_postal_origen_destino
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
                                                
    
    actual_ds = unir_dataframes_total_viajes_por_codigo_postal_origen_destino(total_viajes_por_codigo_postal_origen_ds, total_viajes_por_codigo_postal_destino_ds)

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
                                                
    
    actual_ds = unir_dataframes_total_viajes_por_codigo_postal_origen_destino(total_viajes_por_codigo_postal_origen_ds, total_viajes_por_codigo_postal_destino_ds)
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
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Viajes'])

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
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Viajes'])

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
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Viajes'])

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
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Viajes'])

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
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Viajes'])

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
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Viajes'])

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
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Viajes'])

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
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Viajes'])

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
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Viajes'])

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
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Viajes'])

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
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Viajes'])

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
        ['Codigo_Postal', 'Origen_Destino', 'Cantidad_Total_Viajes'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()      
         