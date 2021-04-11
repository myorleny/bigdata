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

def test_total_viajes_por_codigo_postal_origen_en_blanco(spark_session):
    viajes_didier_data = [(10000,'' , 20105, 5.0, 600),
                        (10005, '', 60101, 100.8, 650),
                        (10198,'' , 11501, 2.0, 0)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_total_viajes_por_codigo_postal_origen(viajes_didier_ds)
    
    actual = actual_ds.count()

    esperado = 0

    assert actual == esperado     

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
    viajes_didier_data = [(10000,20105 , '', 5.0, 600),
                        (10005, 60101, '', 100.8, 650),
                        (10198,11501 , '', 2.0, 0)] 
                                    
    viajes_didier_ds = spark_session.createDataFrame(viajes_didier_data,
                                              ['identificador', 'codigo_postal_origen', 'codigo_postal_destino', 'kilometros', 'precio_kilometro'])
                                                

    viajes_didier_ds.show()

    actual_ds = obtener_total_viajes_por_codigo_postal_destino(viajes_didier_ds)
    
    actual = actual_ds.count()

    esperado = 0

    assert actual == esperado  

# Pruebas para la función unir_dataframes_total_viajes_por_codigo_postal_origen_destino
def test_unir_dataframes_total_viajes_por_codigo_postal_origen_destino(spark_session):
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

def test_unir_dataframes_total_viajes_por_codigo_postal_origen_destino_cantidad_registros(spark_session):
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

    esperado = 5

    assert actual == esperado                         

# def test_KmCiclista_ciclista_ejecuta_misma_ruta_varias_veces_al_dia(spark_session):
#     ciclista_actividad_ruta_data = [(201471234, 'Julio Mora','Alajuela',1,201471234,'2021-02-15',1,'Grecia-Bosque del niño',15.63), 
#                                     (201471234, 'Julio Mora','Alajuela',1,201471234,'2021-02-15',1,'Grecia-Bosque del niño',15.63)]
#     ciclista_actividad_ruta_ds = spark_session.createDataFrame(ciclista_actividad_ruta_data,
#                                               ['cedula', 'nombre_Completo','provincia','codigo_Ruta', 'cedula_Ciclista','fecha','codigo', 'nombre_Ruta','kilometros'])
                                                

#     ciclista_actividad_ruta_ds.show()

#     actual_ds = obtener_kilometros_por_ciclista(ciclista_actividad_ruta_ds)

#     esperado_ds = spark_session.createDataFrame(
#         [
#             (201471234, 'Julio Mora', 1, 'Grecia-Bosque del niño','Alajuela', '2021-02-15', 31.26),
#         ],
#         ['cedula', 'nombre_Completo','codigo','nombre_Ruta', 'provincia','fecha','TotalKilometros'])

#     esperado_ds.show()
#     actual_ds.show()

#     assert actual_ds.collect() == esperado_ds.collect() 

# def test_KmCiclista_kilometros_en_cero(spark_session):
#     ciclista_actividad_ruta_data = [(201471234, 'Julio Mora','Alajuela',1,201471234,'2021-02-15',1,'Grecia-Bosque del niño',0.0), 
#                                     (307530951, 'Marcia Alfaro','Heredia',2,307530951,'2021-02-16',2,'Vuelta a Heredia',50.0)]
#     ciclista_actividad_ruta_ds = spark_session.createDataFrame(ciclista_actividad_ruta_data,
#                                               ['cedula', 'nombre_Completo','provincia','codigo_Ruta', 'cedula_Ciclista','fecha','codigo', 'nombre_Ruta','kilometros'])
                                                

#     ciclista_actividad_ruta_ds.show()

#     actual_ds = obtener_kilometros_por_ciclista(ciclista_actividad_ruta_ds)

#     esperado_ds = spark_session.createDataFrame(
#         [
#             (307530951, 'Marcia Alfaro', 2, 'Vuelta a Heredia', 'Heredia', '2021-02-16', 50.0),
#         ],
#         ['cedula', 'nombre_Completo','codigo','nombre_Ruta', 'provincia','fecha','TotalKilometros'])

#     esperado_ds.show()
#     actual_ds.show()

#     assert actual_ds.collect() == esperado_ds.collect()   

# def test_KmCiclista_kilometros_en_null(spark_session):
#     ciclista_actividad_ruta_data = [(201471234, 'Julio Mora','Alajuela',1,201471234,'2021-02-15',1,'Grecia-Bosque del niño',None), 
#                                     (307530951, 'Marcia Alfaro','Heredia',2,307530951,'2021-02-16',2,'Vuelta a Heredia',50.0)]
#     ciclista_actividad_ruta_ds = spark_session.createDataFrame(ciclista_actividad_ruta_data,
#                                               ['cedula', 'nombre_Completo','provincia','codigo_Ruta', 'cedula_Ciclista','fecha','codigo', 'nombre_Ruta','kilometros'])
                                                

#     ciclista_actividad_ruta_ds.show()

#     actual_ds = obtener_kilometros_por_ciclista(ciclista_actividad_ruta_ds)

#     esperado_ds = spark_session.createDataFrame(
#         [
#             (307530951, 'Marcia Alfaro', 2, 'Vuelta a Heredia', 'Heredia', '2021-02-16', 50.0),
#         ],
#         ['cedula', 'nombre_Completo','codigo','nombre_Ruta', 'provincia','fecha','TotalKilometros'])

#     esperado_ds.show()
#     actual_ds.show()

#     assert actual_ds.collect() == esperado_ds.collect()  

# def test_KmCiclista_kilometros_negativos(spark_session):
#     ciclista_actividad_ruta_data = [(201471234, 'Julio Mora','Alajuela',1,201471234,'2021-02-15',1,'Grecia-Bosque del niño',-15.0), 
#                                     (307530951, 'Marcia Alfaro','Heredia',2,307530951,'2021-02-16',2,'Vuelta a Heredia',50.5)]
#     ciclista_actividad_ruta_ds = spark_session.createDataFrame(ciclista_actividad_ruta_data,
#                                               ['cedula', 'nombre_Completo','provincia','codigo_Ruta', 'cedula_Ciclista','fecha','codigo', 'nombre_Ruta','kilometros'])
                                                

#     ciclista_actividad_ruta_ds.show()

#     actual_ds = obtener_kilometros_por_ciclista(ciclista_actividad_ruta_ds)

#     esperado_ds = spark_session.createDataFrame(
#         [
#             (307530951, 'Marcia Alfaro', 2, 'Vuelta a Heredia', 'Heredia', '2021-02-16', 50.5),
#         ],
#         ['cedula', 'nombre_Completo','codigo','nombre_Ruta', 'provincia','fecha','TotalKilometros'])

#     esperado_ds.show()
#     actual_ds.show()

#     assert actual_ds.collect() == esperado_ds.collect()     

# #Pruebas para la función obtener_topN_ciclistas_por_provincia_en_total_de_kilometros    
# def test_top1_ciclistas_por_provincia_total_km_un_registro_por_ciclista(spark_session):
#     ciclistas_kilometros_data = [(103330444, 'Yahaira Alfaro', 9, 'Ruta Escazú', 'San José','2021-03-19', 9.1),
#                                 (102580852, 'Julia Mora', 4, 'Vuelta Volcán Arenal', 'San José','2021-01-15', 60.0),
#                                 (202220333, 'Eduardo Jiménez', 4, 'Vuelta Volcán Arenal', 'Alajuela', '2021-03-31', 65.0),
#                                 (201110222, 'María Gómez', 10, 'UCR-Sabana', 'Alajuela','2021-02-01',26.0),
#                                 (204440555, 'Allan Vindas', 7, 'Tibas-Irazu-Tibas', 'Alajuela','2021-03-21', 52.0)]

#     ciclistas_kilometros_ds = spark_session.createDataFrame(ciclistas_kilometros_data,
#                                               ['cedula', 'nombre_Completo','codigo','nombre_Ruta', 'provincia','fecha','TotalKilometros'])
                                                

#     ciclistas_kilometros_ds.show()

#     actual_ds = obtener_topN_ciclistas_por_provincia_en_total_de_kilometros(ciclistas_kilometros_ds, 1)

#     esperado_ds = spark_session.createDataFrame(
#         [
#             ('Total de Km', 'San José', 102580852, 'Julia Mora', 60.0, 1),
#             ('Total de Km', 'Alajuela', 202220333, 'Eduardo Jiménez', 65.0, 1),
#         ],
#         ['Tipo_Top_N_Ciclistas_Por_Provincia', 'provincia','cedula','nombre_Completo', 'Valor','Posicion_Por_Provincia'])

#     esperado_ds.show()
#     actual_ds.show()

#     assert actual_ds.collect() == esperado_ds.collect() 

# def test_top1_ciclistas_por_provincia_total_km_varios_registros_por_ciclista(spark_session):
#     ciclistas_kilometros_data = [  (103330444, 'Yahaira Alfaro', 9, 'Ruta Escazú', 'San José','2021-03-19', 9.1),
#                                 (103330444, 'Yahaira Alfaro', 4, 'Vuelta Volcán Arenal', 'San José','2021-03-20', 60.0),
#                                 (102580852, 'Julia Mora', 4, 'Vuelta Volcán Arenal', 'San José','2021-01-15', 60.0),
#                                 (102580852, 'Julia Mora', 11, 'UCR-San José Centro', 'San José','2021-01-16', 5.0),
#                                 (202220333, 'Eduardo Jiménez', 4, 'Vuelta Volcán Arenal', 'Alajuela', '2021-03-30', 65.0),
#                                 (202220333, 'Eduardo Jiménez', 10, 'UCR-Sabana', 'Alajuela', '2021-03-31', 26.0),
#                                 (201110222, 'María Gómez', 10, 'UCR-Sabana', 'Alajuela','2021-02-01',26.0),
#                                 (201110222, 'María Gómez', 7, 'Tibas-Irazu-Tibas', 'Alajuela','2021-02-02',52.0),
#                                 (204440555, 'Allan Vindas', 7, 'Tibas-Irazu-Tibas', 'Alajuela','2021-03-21', 52.0),
#                                 (204440555, 'Allan Vindas', 9, 'Ruta Escazú', 'Alajuela','2021-03-22', 9.1)]

#     ciclistas_kilometros_ds = spark_session.createDataFrame(ciclistas_kilometros_data,
#                                               ['cedula', 'nombre_Completo','codigo','nombre_Ruta', 'provincia','fecha','TotalKilometros'])
                                                

#     ciclistas_kilometros_ds.show()

#     actual_ds = obtener_topN_ciclistas_por_provincia_en_total_de_kilometros(ciclistas_kilometros_ds, 1)

#     esperado_ds = spark_session.createDataFrame(
#         [
#             ('Total de Km', 'San José', 103330444, 'Yahaira Alfaro', 69.1, 1),
#             ('Total de Km', 'Alajuela', 202220333, 'Eduardo Jiménez', 91.0, 1),
#         ],
#         ['Tipo_Top_N_Ciclistas_Por_Provincia', 'provincia','cedula','nombre_Completo', 'Valor','Posicion_Por_Provincia'])

#     esperado_ds.show()
#     actual_ds.show()

#     assert actual_ds.collect() == esperado_ds.collect()     

# def test_top1_ciclistas_por_provincia_total_km_ciclistas_Empatados(spark_session):
#     ciclistas_kilometros_data = [(103330444, 'Yahaira Alfaro', 9, 'Ruta Escazú', 'San José','2021-03-19', 9.1),
#                                 (103330444, 'Yahaira Alfaro', 4, 'Vuelta Volcán Arenal', 'San José','2021-03-20', 60.0),
#                                 (102580852, 'Julia Mora', 4, 'Vuelta Volcán Arenal', 'San José','2021-01-15', 60.0),
#                                 (102580852, 'Julia Mora', 9, 'Ruta Escazú', 'San José','2021-01-16', 9.1),
#                                 (201110222, 'María Gómez', 10, 'UCR-Sabana', 'Alajuela','2021-02-01',26.0),
#                                 (201110222, 'María Gómez', 7, 'Tibas-Irazu-Tibas', 'Alajuela','2021-02-02',52.0),
#                                 (204440555, 'Allan Vindas', 10, 'UCR-Sabana', 'Alajuela','2021-02-01',26.0),
#                                 (204440555, 'Allan Vindas', 7, 'Tibas-Irazu-Tibas', 'Alajuela','2021-02-02', 52.0)]

#     ciclistas_kilometros_ds = spark_session.createDataFrame(ciclistas_kilometros_data,
#                                               ['cedula', 'nombre_Completo','codigo','nombre_Ruta', 'provincia','fecha','TotalKilometros'])
                                                

#     ciclistas_kilometros_ds.show()

#     actual_ds = obtener_topN_ciclistas_por_provincia_en_total_de_kilometros(ciclistas_kilometros_ds, 1)

#     esperado_ds = spark_session.createDataFrame(
#         [
#             ('Total de Km', 'San José', 102580852, 'Julia Mora', 69.1, 1),
#             ('Total de Km', 'Alajuela', 201110222, 'María Gómez', 78.0, 1),
#         ],
#         ['Tipo_Top_N_Ciclistas_Por_Provincia', 'provincia','cedula','nombre_Completo', 'Valor','Posicion_Por_Provincia'])

#     esperado_ds.show()
#     actual_ds.show()

#     assert actual_ds.collect() == esperado_ds.collect()     

# def test_top2_ciclistas_por_provincia_total_km(spark_session):
#     ciclistas_kilometros_data = [(103330444, 'Yahaira Alfaro', 9, 'Ruta Escazú', 'San José','2021-03-19', 9.1),
#                                 (102580852, 'Julia Mora', 4, 'Vuelta Volcán Arenal', 'San José','2021-01-15', 60.0),
#                                 (103210123,'Juan Pérez', 3,'Cartago-San José', 'San José', '2021-01-01', 10.0),
#                                 (202220333, 'Eduardo Jiménez', 4, 'Vuelta Volcán Arenal', 'Alajuela', '2021-03-31', 65.0),
#                                 (202220333, 'Eduardo Jiménez', 9, 'Ruta Escazú', 'Alajuela','2021-03-19', 9.1),
#                                 (201110222, 'María Gómez', 10, 'UCR-Sabana', 'Alajuela','2021-02-01',26.0),
#                                 (204440555, 'Allan Vindas', 7, 'Tibas-Irazu-Tibas', 'Alajuela','2021-03-21', 52.0),
# ]

#     ciclistas_kilometros_ds = spark_session.createDataFrame(ciclistas_kilometros_data,
#                                               ['cedula', 'nombre_Completo','codigo','nombre_Ruta', 'provincia','fecha','TotalKilometros'])
                                                

#     ciclistas_kilometros_ds.show()

#     actual_ds = obtener_topN_ciclistas_por_provincia_en_total_de_kilometros(ciclistas_kilometros_ds, 2)

#     esperado_ds = spark_session.createDataFrame(
#         [
#             ('Total de Km', 'San José', 102580852, 'Julia Mora', 60.0, 1),
#             ('Total de Km', 'San José', 103210123,'Juan Pérez', 10.0, 2),
#             ('Total de Km', 'Alajuela', 202220333, 'Eduardo Jiménez', 74.1, 1),
#             ('Total de Km', 'Alajuela', 204440555, 'Allan Vindas', 52.0, 2),
#         ],
#         ['Tipo_Top_N_Ciclistas_Por_Provincia', 'provincia','cedula','nombre_Completo', 'Valor','Posicion_Por_Provincia'])

#     esperado_ds.show()
#     actual_ds.show()

#     assert actual_ds.collect() == esperado_ds.collect()  

# def test_top5_ciclistas_por_provincia_total_km(spark_session):
#     ciclistas_kilometros_data = [(103330444, 'Yahaira Alfaro', 9, 'Ruta Escazú', 'San José','2021-03-19', 9.1),
#                                 (102580852, 'Julia Mora', 4, 'Vuelta Volcán Arenal', 'San José','2021-01-15', 60.0),
#                                 (202220333, 'Eduardo Jiménez', 4, 'Vuelta Volcán Arenal', 'Alajuela', '2021-03-31', 65.0),
#                                 (206060256, 'Mario Ugalde', 9, 'Ruta Escazú', 'Alajuela','2021-03-19', 9.1),
#                                 (201110222, 'María Gómez', 10, 'UCR-Sabana', 'Alajuela','2021-02-01',26.0),
#                                 (204440555, 'Allan Vindas', 7, 'Tibas-Irazu-Tibas', 'Alajuela','2021-03-21', 52.0),
#                                 (202560503, 'Gerardo Alfaro', 7, 'San José-Grecia', 'Alajuela','2021-03-21', 50.0),
#                                 (103210123,'Juan Pérez', 3,'Cartago-San José', 'Limón', '2021-01-01', 10.0)
# ]

#     ciclistas_kilometros_ds = spark_session.createDataFrame(ciclistas_kilometros_data,
#                                               ['cedula', 'nombre_Completo','codigo','nombre_Ruta', 'provincia','fecha','TotalKilometros'])
                                                

#     ciclistas_kilometros_ds.show()

#     actual_ds = obtener_topN_ciclistas_por_provincia_en_total_de_kilometros(ciclistas_kilometros_ds, 5)

#     esperado_ds = spark_session.createDataFrame(
#         [
#             ('Total de Km', 'San José', 102580852, 'Julia Mora', 60.0, 1),
#             ('Total de Km', 'San José', 103330444, 'Yahaira Alfaro', 9.1, 2),
#             ('Total de Km', 'Alajuela', 202220333, 'Eduardo Jiménez', 65.0, 1),
#             ('Total de Km', 'Alajuela', 204440555, 'Allan Vindas', 52.0, 2),
#             ('Total de Km', 'Alajuela', 202560503, 'Gerardo Alfaro', 50.0, 3),
#             ('Total de Km', 'Alajuela', 201110222, 'María Gómez', 26.0, 4),
#             ('Total de Km', 'Alajuela', 206060256, 'Mario Ugalde', 9.1, 5),
#             ('Total de Km', 'Limón', 103210123,'Juan Pérez', 10.0, 1),
#         ],
#         ['Tipo_Top_N_Ciclistas_Por_Provincia', 'provincia','cedula','nombre_Completo', 'Valor','Posicion_Por_Provincia'])

#     esperado_ds.show()
#     actual_ds.show()

#     assert actual_ds.collect() == esperado_ds.collect()       

# #Pruebas para la función obtener_topN_ciclistas_por_provincia_en_promedio_de_kilometros_por_dia    
# def test_top1_ciclistas_por_provincia_promedio_KmDia_un_registro_por_ciclista(spark_session):
#     ciclistas_kilometros_data = [(103330444, 'Yahaira Alfaro', 9, 'Ruta Escazú', 'San José','2021-03-19', 9.1),
#                                 (102580852, 'Julia Mora', 4, 'Vuelta Volcán Arenal', 'San José','2021-01-15', 60.0),
#                                 (202220333, 'Eduardo Jiménez', 4, 'Vuelta Volcán Arenal', 'Alajuela', '2021-03-31', 65.0),
#                                 (201110222, 'María Gómez', 10, 'UCR-Sabana', 'Alajuela','2021-02-01',26.0),
#                                 (204440555, 'Allan Vindas', 7, 'Tibas-Irazu-Tibas', 'Alajuela','2021-03-21', 52.0)]

#     ciclistas_kilometros_ds = spark_session.createDataFrame(ciclistas_kilometros_data,
#                                               ['cedula', 'nombre_Completo','codigo','nombre_Ruta', 'provincia','fecha','TotalKilometros'])
                                                

#     ciclistas_kilometros_ds.show()

#     actual_ds = obtener_topN_ciclistas_por_provincia_en_promedio_de_kilometros_por_dia(ciclistas_kilometros_ds, 1)

#     esperado_ds = spark_session.createDataFrame(
#         [
#             ('Promedio de Km/día', 'San José', 102580852, 'Julia Mora', 60.0, 1),
#             ('Promedio de Km/día', 'Alajuela', 202220333, 'Eduardo Jiménez', 65.0, 1),
#         ],
#         ['Tipo_Top_N_Ciclistas_Por_Provincia', 'provincia','cedula','nombre_Completo', 'Valor','Posicion_Por_Provincia'])

#     esperado_ds.show()
#     actual_ds.show()

#     assert actual_ds.collect() == esperado_ds.collect() 

# def test_top1_ciclistas_por_provincia_promedio_KmDia_varios_registros_por_ciclista(spark_session):
#     ciclistas_kilometros_data = [  (103330444, 'Yahaira Alfaro', 9, 'Ruta Escazú', 'San José','2021-03-19', 9.1),
#                                 (103330444, 'Yahaira Alfaro', 4, 'Vuelta Volcán Arenal', 'San José','2021-03-20', 60.0),
#                                 (102580852, 'Julia Mora', 4, 'Vuelta Volcán Arenal', 'San José','2021-01-15', 60.0),
#                                 (102580852, 'Julia Mora', 11, 'UCR-San José Centro', 'San José','2021-01-16', 5.0),
#                                 (202220333, 'Eduardo Jiménez', 4, 'Vuelta Volcán Arenal', 'Alajuela', '2021-03-30', 65.0),
#                                 (202220333, 'Eduardo Jiménez', 10, 'UCR-Sabana', 'Alajuela', '2021-03-31', 26.0),
#                                 (201110222, 'María Gómez', 10, 'UCR-Sabana', 'Alajuela','2021-02-01',26.0),
#                                 (201110222, 'María Gómez', 7, 'Tibas-Irazu-Tibas', 'Alajuela','2021-02-02',52.0),
#                                 (204440555, 'Allan Vindas', 7, 'Tibas-Irazu-Tibas', 'Alajuela','2021-03-21', 52.0),
#                                 (204440555, 'Allan Vindas', 9, 'Ruta Escazú', 'Alajuela','2021-03-22', 9.1)]

#     ciclistas_kilometros_ds = spark_session.createDataFrame(ciclistas_kilometros_data,
#                                               ['cedula', 'nombre_Completo','codigo','nombre_Ruta', 'provincia','fecha','TotalKilometros'])
                                                

#     ciclistas_kilometros_ds.show()

#     actual_ds = obtener_topN_ciclistas_por_provincia_en_promedio_de_kilometros_por_dia(ciclistas_kilometros_ds, 1)

#     esperado_ds = spark_session.createDataFrame(
#         [
#             ('Promedio de Km/día', 'San José', 103330444, 'Yahaira Alfaro', 34.55, 1),
#             ('Promedio de Km/día', 'Alajuela', 202220333, 'Eduardo Jiménez', 45.5, 1),
#         ],
#         ['Tipo_Top_N_Ciclistas_Por_Provincia', 'provincia','cedula','nombre_Completo', 'Valor','Posicion_Por_Provincia'])

#     esperado_ds.show()
#     actual_ds.show()

#     assert actual_ds.collect() == esperado_ds.collect()     

# def test_top1_ciclistas_por_provincia_promedio_KmDia_ciclistas_Empatados(spark_session):
#     ciclistas_kilometros_data = [(103330444, 'Yahaira Alfaro', 9, 'Ruta Escazú', 'San José','2021-03-19', 9.1),
#                                 (103330444, 'Yahaira Alfaro', 4, 'Vuelta Volcán Arenal', 'San José','2021-03-20', 60.0),
#                                 (102580852, 'Julia Mora', 4, 'Vuelta Volcán Arenal', 'San José','2021-01-15', 60.0),
#                                 (102580852, 'Julia Mora', 9, 'Ruta Escazú', 'San José','2021-01-16', 9.1),
#                                 (201110222, 'María Gómez', 10, 'UCR-Sabana', 'Alajuela','2021-02-01',26.0),
#                                 (201110222, 'María Gómez', 7, 'Tibas-Irazu-Tibas', 'Alajuela','2021-02-02',52.0),
#                                 (204440555, 'Allan Vindas', 10, 'UCR-Sabana', 'Alajuela','2021-02-01',26.0),
#                                 (204440555, 'Allan Vindas', 7, 'Tibas-Irazu-Tibas', 'Alajuela','2021-02-02', 52.0)]

#     ciclistas_kilometros_ds = spark_session.createDataFrame(ciclistas_kilometros_data,
#                                               ['cedula', 'nombre_Completo','codigo','nombre_Ruta', 'provincia','fecha','TotalKilometros'])
                                                

#     ciclistas_kilometros_ds.show()

#     actual_ds = obtener_topN_ciclistas_por_provincia_en_promedio_de_kilometros_por_dia(ciclistas_kilometros_ds, 1)

#     esperado_ds = spark_session.createDataFrame(
#         [
#             ('Promedio de Km/día', 'San José', 102580852, 'Julia Mora', 34.55, 1),
#             ('Promedio de Km/día', 'Alajuela', 201110222, 'María Gómez', 39.0, 1),
#         ],
#         ['Tipo_Top_N_Ciclistas_Por_Provincia', 'provincia','cedula','nombre_Completo', 'Valor','Posicion_Por_Provincia'])

#     esperado_ds.show()
#     actual_ds.show()

#     assert actual_ds.collect() == esperado_ds.collect()     

# def test_top2_ciclistas_por_provincia_promedio_KmDia(spark_session):
#     ciclistas_kilometros_data = [(103330444, 'Yahaira Alfaro', 9, 'Ruta Escazú', 'San José','2021-03-19', 9.1),
#                                 (102580852, 'Julia Mora', 4, 'Vuelta Volcán Arenal', 'San José','2021-01-15', 60.0),
#                                 (103210123,'Juan Pérez', 4, 'Vuelta Volcán Arenal', 'San José','2021-01-15', 60.0),
#                                 (103210123,'Juan Pérez', 3,'Cartago-San José', 'San José', '2021-01-01', 10.0),
#                                 (202220333, 'Eduardo Jiménez', 4, 'Vuelta Volcán Arenal', 'Alajuela', '2021-03-31', 65.0),
#                                 (202220333, 'Eduardo Jiménez', 9, 'Ruta Escazú', 'Alajuela','2021-03-19', 9.1),
#                                 (201110222, 'María Gómez', 10, 'UCR-Sabana', 'Alajuela','2021-02-01',26.0),
#                                 (204440555, 'Allan Vindas', 7, 'Tibas-Irazu-Tibas', 'Alajuela','2021-03-21', 52.0),
# ]

#     ciclistas_kilometros_ds = spark_session.createDataFrame(ciclistas_kilometros_data,
#                                               ['cedula', 'nombre_Completo','codigo','nombre_Ruta', 'provincia','fecha','TotalKilometros'])
                                                

#     ciclistas_kilometros_ds.show()

#     actual_ds = obtener_topN_ciclistas_por_provincia_en_promedio_de_kilometros_por_dia(ciclistas_kilometros_ds, 2)

#     esperado_ds = spark_session.createDataFrame(
#         [
#             ('Promedio de Km/día', 'San José', 102580852, 'Julia Mora', 60.0, 1),
#             ('Promedio de Km/día', 'San José', 103210123,'Juan Pérez', 35.0, 2),
#             ('Promedio de Km/día', 'Alajuela', 204440555, 'Allan Vindas', 52.0, 1),
#             ('Promedio de Km/día', 'Alajuela', 202220333, 'Eduardo Jiménez', 37.05, 2),            
#         ],
#         ['Tipo_Top_N_Ciclistas_Por_Provincia', 'provincia','cedula','nombre_Completo', 'Valor','Posicion_Por_Provincia'])

#     esperado_ds.show()
#     actual_ds.show()

#     assert actual_ds.collect() == esperado_ds.collect()  

# def test_top5_ciclistas_por_provincia_promedio_KmDia(spark_session):
#     ciclistas_kilometros_data = [(103330444, 'Yahaira Alfaro', 9, 'Ruta Escazú', 'San José','2021-03-19', 9.1),
#                                 (102580852, 'Julia Mora', 4, 'Vuelta Volcán Arenal', 'San José','2021-01-15', 60.0),
#                                 (202220333, 'Eduardo Jiménez', 4, 'Vuelta Volcán Arenal', 'Alajuela', '2021-03-31', 65.0),
#                                 (202220333, 'Eduardo Jiménez', 9, 'Ruta Escazú', 'Alajuela','2021-03-19', 9.1),
#                                 (206060256, 'Mario Ugalde', 9, 'Ruta Escazú', 'Alajuela','2021-03-19', 9.1),
#                                 (201110222, 'María Gómez', 10, 'UCR-Sabana', 'Alajuela','2021-02-01',26.0),
#                                 (204440555, 'Allan Vindas', 7, 'Tibas-Irazu-Tibas', 'Alajuela','2021-03-21', 52.0),
#                                 (202560503, 'Gerardo Alfaro', 7, 'San José-Grecia', 'Alajuela','2021-03-21', 50.0),
#                                 (103210123,'Juan Pérez', 3,'Cartago-San José', 'Limón', '2021-01-01', 10.0)
# ]

#     ciclistas_kilometros_ds = spark_session.createDataFrame(ciclistas_kilometros_data,
#                                               ['cedula', 'nombre_Completo','codigo','nombre_Ruta', 'provincia','fecha','TotalKilometros'])
                                                

#     ciclistas_kilometros_ds.show()

#     actual_ds = obtener_topN_ciclistas_por_provincia_en_promedio_de_kilometros_por_dia(ciclistas_kilometros_ds, 5)

#     esperado_ds = spark_session.createDataFrame(
#         [
#             ('Promedio de Km/día', 'San José', 102580852, 'Julia Mora', 60.0, 1),
#             ('Promedio de Km/día', 'San José', 103330444, 'Yahaira Alfaro', 9.1, 2),
#             ('Promedio de Km/día', 'Alajuela', 204440555, 'Allan Vindas', 52.0, 1),
#             ('Promedio de Km/día', 'Alajuela', 202560503, 'Gerardo Alfaro', 50.0, 2),
#             ('Promedio de Km/día', 'Alajuela', 202220333, 'Eduardo Jiménez', 37.05, 3),            
#             ('Promedio de Km/día', 'Alajuela', 201110222, 'María Gómez', 26.0, 4),
#             ('Promedio de Km/día', 'Alajuela', 206060256, 'Mario Ugalde', 9.1, 5),
#             ('Promedio de Km/día', 'Limón', 103210123,'Juan Pérez', 10.0, 1),
#         ],
#         ['Tipo_Top_N_Ciclistas_Por_Provincia', 'provincia','cedula','nombre_Completo', 'Valor','Posicion_Por_Provincia'])

#     esperado_ds.show()
#     actual_ds.show()

#     assert actual_ds.collect() == esperado_ds.collect()       

# # Pruebas para la función unir_dataframes_Top_N_ciclistas_por_provincia
# def test_unir_dataframes_Top_1_ciclistas_por_provincia(spark_session):
#     provincia_ciclistas_kilometros_total_data = [('Total de Km', 'San José', 102580852, 'Julia Mora', 69.1, 1),
#                                                 ('Total de Km', 'Alajuela', 202220333, 'Eduardo Jiménez', 74.1, 1)]

#     provincia_ciclistas_kilometros_total_ds = spark_session.createDataFrame(provincia_ciclistas_kilometros_total_data,
#                                               ['Tipo_Top_N_Ciclistas_Por_Provincia', 'provincia','cedula','nombre_Completo', 'Valor','Posicion_Por_Provincia'])
                                                
#     provincia_ciclistas_kilometros_total_ds.show()

#     provincia_ciclistas_kilometros_promedio_data = [('Promedio de Km/día', 'San José', 102580852, 'Julia Mora', 34.55, 1),
#                                                     ('Promedio de Km/día', 'Alajuela', 204440555, 'Allan Vindas', 52.0, 1)]

#     provincia_ciclistas_kilometros_promedio_ds = spark_session.createDataFrame(provincia_ciclistas_kilometros_promedio_data,
#                                               ['Tipo_Top_N_Ciclistas_Por_Provincia', 'provincia','cedula','nombre_Completo', 'Valor','Posicion_Por_Provincia'])
                                                
#     provincia_ciclistas_kilometros_promedio_ds.show()    

#     actual_ds = unir_dataframes_Top_N_ciclistas_por_provincia(provincia_ciclistas_kilometros_total_ds, provincia_ciclistas_kilometros_promedio_ds)

#     esperado_ds = spark_session.createDataFrame(
#         [
#             ('Total de Km', 'San José', 102580852, 'Julia Mora', 69.1, 1),
#             ('Total de Km', 'Alajuela', 202220333, 'Eduardo Jiménez', 74.1, 1),
#             ('Promedio de Km/día', 'San José', 102580852, 'Julia Mora', 34.55, 1),
#             ('Promedio de Km/día', 'Alajuela', 204440555, 'Allan Vindas', 52.0, 1),
#         ],
#         ['Tipo_Top_N_Ciclistas_Por_Provincia', 'provincia','cedula','nombre_Completo', 'Valor','Posicion_Por_Provincia'])

#     esperado_ds.show()
#     actual_ds.show()

#     assert actual_ds.collect() == esperado_ds.collect()   

# def test_unir_dataframes_Top_2_ciclistas_por_provincia(spark_session):
#     provincia_ciclistas_kilometros_total_data = [('Total de Km', 'San José', 102580852, 'Julia Mora', 60.0, 1),
#                                                 ('Total de Km', 'San José', 103210123,'Juan Pérez', 10.0, 2),
#                                                 ('Total de Km', 'Alajuela', 202220333, 'Eduardo Jiménez', 74.1, 1),
#                                                 ('Total de Km', 'Alajuela', 204440555, 'Allan Vindas', 52.0, 2)]

#     provincia_ciclistas_kilometros_total_ds = spark_session.createDataFrame(provincia_ciclistas_kilometros_total_data,
#                                               ['Tipo_Top_N_Ciclistas_Por_Provincia', 'provincia','cedula','nombre_Completo', 'Valor','Posicion_Por_Provincia'])
                                                
#     provincia_ciclistas_kilometros_total_ds.show()

#     provincia_ciclistas_kilometros_promedio_data = [('Promedio de Km/día', 'San José', 102580852, 'Julia Mora', 60.0, 1),
#                                                     ('Promedio de Km/día', 'San José', 103210123,'Juan Pérez', 35.0, 2),
#                                                     ('Promedio de Km/día', 'Alajuela', 204440555, 'Allan Vindas', 52.0, 1),
#                                                     ('Promedio de Km/día', 'Alajuela', 202220333, 'Eduardo Jiménez', 37.05, 2)]

#     provincia_ciclistas_kilometros_promedio_ds = spark_session.createDataFrame(provincia_ciclistas_kilometros_promedio_data,
#                                               ['Tipo_Top_N_Ciclistas_Por_Provincia', 'provincia','cedula','nombre_Completo', 'Valor','Posicion_Por_Provincia'])
                                                
#     provincia_ciclistas_kilometros_promedio_ds.show()    

#     actual_ds = unir_dataframes_Top_N_ciclistas_por_provincia(provincia_ciclistas_kilometros_total_ds, provincia_ciclistas_kilometros_promedio_ds)

#     esperado_ds = spark_session.createDataFrame(
#         [
#             ('Total de Km', 'San José', 102580852, 'Julia Mora', 60.0, 1),
#             ('Total de Km', 'San José', 103210123,'Juan Pérez', 10.0, 2),
#             ('Total de Km', 'Alajuela', 202220333, 'Eduardo Jiménez', 74.1, 1),
#             ('Total de Km', 'Alajuela', 204440555, 'Allan Vindas', 52.0, 2),
#             ('Promedio de Km/día', 'San José', 102580852, 'Julia Mora', 60.0, 1),
#             ('Promedio de Km/día', 'San José', 103210123,'Juan Pérez', 35.0, 2),
#             ('Promedio de Km/día', 'Alajuela', 204440555, 'Allan Vindas', 52.0, 1),
#             ('Promedio de Km/día', 'Alajuela', 202220333, 'Eduardo Jiménez', 37.05, 2)            
#         ],
#         ['Tipo_Top_N_Ciclistas_Por_Provincia', 'provincia','cedula','nombre_Completo', 'Valor','Posicion_Por_Provincia'])

#     esperado_ds.show()
#     actual_ds.show()

#     assert actual_ds.collect() == esperado_ds.collect()       

# def test_unir_dataframes_Top_5_ciclistas_por_provincia(spark_session):
#     provincia_ciclistas_kilometros_total_data = [('Total de Km', 'San José', 102580852, 'Julia Mora', 60.0, 1),
#                                                 ('Total de Km', 'San José', 103330444, 'Yahaira Alfaro', 9.1, 2),
#                                                 ('Total de Km', 'Alajuela', 202220333, 'Eduardo Jiménez', 65.0, 1),
#                                                 ('Total de Km', 'Alajuela', 204440555, 'Allan Vindas', 52.0, 2),
#                                                 ('Total de Km', 'Alajuela', 202560503, 'Gerardo Alfaro', 50.0, 3),
#                                                 ('Total de Km', 'Alajuela', 201110222, 'María Gómez', 26.0, 4),
#                                                 ('Total de Km', 'Alajuela', 206060256, 'Mario Ugalde', 9.1, 5),
#                                                 ('Total de Km', 'Limón', 103210123,'Juan Pérez', 10.0, 1)]

#     provincia_ciclistas_kilometros_total_ds = spark_session.createDataFrame(provincia_ciclistas_kilometros_total_data,
#                                               ['Tipo_Top_N_Ciclistas_Por_Provincia', 'provincia','cedula','nombre_Completo', 'Valor','Posicion_Por_Provincia'])
                                                
#     provincia_ciclistas_kilometros_total_ds.show()

#     provincia_ciclistas_kilometros_promedio_data = [ ('Promedio de Km/día', 'San José', 102580852, 'Julia Mora', 60.0, 1),
#                                                     ('Promedio de Km/día', 'San José', 103330444, 'Yahaira Alfaro', 9.1, 2),
#                                                     ('Promedio de Km/día', 'Alajuela', 204440555, 'Allan Vindas', 52.0, 1),
#                                                     ('Promedio de Km/día', 'Alajuela', 202560503, 'Gerardo Alfaro', 50.0, 2),
#                                                     ('Promedio de Km/día', 'Alajuela', 202220333, 'Eduardo Jiménez', 37.05, 3),            
#                                                     ('Promedio de Km/día', 'Alajuela', 201110222, 'María Gómez', 26.0, 4),
#                                                     ('Promedio de Km/día', 'Alajuela', 206060256, 'Mario Ugalde', 9.1, 5),
#                                                     ('Promedio de Km/día', 'Limón', 103210123,'Juan Pérez', 10.0, 1)]

#     provincia_ciclistas_kilometros_promedio_ds = spark_session.createDataFrame(provincia_ciclistas_kilometros_promedio_data,
#                                               ['Tipo_Top_N_Ciclistas_Por_Provincia', 'provincia','cedula','nombre_Completo', 'Valor','Posicion_Por_Provincia'])
                                                
#     provincia_ciclistas_kilometros_promedio_ds.show()    

#     actual_ds = unir_dataframes_Top_N_ciclistas_por_provincia(provincia_ciclistas_kilometros_total_ds, provincia_ciclistas_kilometros_promedio_ds)

#     esperado_ds = spark_session.createDataFrame(
#         [
#             ('Total de Km', 'San José', 102580852, 'Julia Mora', 60.0, 1),
#             ('Total de Km', 'San José', 103330444, 'Yahaira Alfaro', 9.1, 2),
#             ('Total de Km', 'Alajuela', 202220333, 'Eduardo Jiménez', 65.0, 1),
#             ('Total de Km', 'Alajuela', 204440555, 'Allan Vindas', 52.0, 2),
#             ('Total de Km', 'Alajuela', 202560503, 'Gerardo Alfaro', 50.0, 3),
#             ('Total de Km', 'Alajuela', 201110222, 'María Gómez', 26.0, 4),
#             ('Total de Km', 'Alajuela', 206060256, 'Mario Ugalde', 9.1, 5),
#             ('Total de Km', 'Limón', 103210123,'Juan Pérez', 10.0, 1),
#             ('Promedio de Km/día', 'San José', 102580852, 'Julia Mora', 60.0, 1),
#             ('Promedio de Km/día', 'San José', 103330444, 'Yahaira Alfaro', 9.1, 2),
#             ('Promedio de Km/día', 'Alajuela', 204440555, 'Allan Vindas', 52.0, 1),
#             ('Promedio de Km/día', 'Alajuela', 202560503, 'Gerardo Alfaro', 50.0, 2),
#             ('Promedio de Km/día', 'Alajuela', 202220333, 'Eduardo Jiménez', 37.05, 3),            
#             ('Promedio de Km/día', 'Alajuela', 201110222, 'María Gómez', 26.0, 4),
#             ('Promedio de Km/día', 'Alajuela', 206060256, 'Mario Ugalde', 9.1, 5),
#             ('Promedio de Km/día', 'Limón', 103210123,'Juan Pérez', 10.0, 1),           
#         ],
#         ['Tipo_Top_N_Ciclistas_Por_Provincia', 'provincia','cedula','nombre_Completo', 'Valor','Posicion_Por_Provincia'])

#     esperado_ds.show()
#     actual_ds.show()

#     assert actual_ds.collect() == esperado_ds.collect()          