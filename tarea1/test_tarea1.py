from .tarea1 import unir_dataframes


def test_union_normal_dataframes(spark_session):
    ciclista_data = [(201471234, 'Julio Mora','Alajuela'), (307530951, 'Marcia Alfaro','Heredia')]
    ciclista_ds = spark_session.createDataFrame(ciclista_data,
                                              ['cedula', 'nombre_Completo','provincia'])
    ruta_data = [(1, 'Grecia-Bosque del niño',15), (2, 'Vuelta a Heredia',50)]
    ruta_ds = spark_session.createDataFrame(ruta_data,
                                               ['codigo', 'nombre_Ruta','kilometros'])
    actividad_data = [(1, 201471234, '2021-02-15'), (2, 307530951, '2021-03-01')]
    actividad_ds = spark_session.createDataFrame(actividad_data,
                                              ['codigo_Ruta', 'cedula_Ciclista','fecha'])                                               

    ciclista_ds.show()
    ruta_ds.show()
    actividad_ds.show()

    actual_ds = unir_dataframes(ciclista_ds, ruta_ds, actividad_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (201471234, 'Julio Mora','Alajuela', 1, 201471234, '2021-02-15', 1, 'Grecia-Bosque del niño',15),
            (307530951, 'Marcia Alfaro','Heredia', 2, 307530951, '2021-03-01', 2, 'Vuelta a Heredia',50),
        ],
        ['cedula', 'nombre_Completo','provincia','codigo_Ruta', 'cedula_Ciclista','fecha','codigo', 'nombre_Ruta','kilometros'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()

def test_union_ciclista_no_tiene_actividad(spark_session):
    ciclista_data = [(201471234, 'Julio Mora','Alajuela'), (307530951, 'Marcia Alfaro','Heredia')]
    ciclista_ds = spark_session.createDataFrame(ciclista_data,
                                              ['cedula', 'nombre_Completo','provincia'])
    
    ruta_data = [(1, 'Grecia-Bosque del niño',15)]
    ruta_ds = spark_session.createDataFrame(ruta_data,
                                               ['codigo', 'nombre_Ruta','kilometros'])
    
    actividad_data = [(1, 201471234, '2021-02-15')]
    actividad_ds = spark_session.createDataFrame(actividad_data,
                                              ['codigo_Ruta', 'cedula_Ciclista','fecha'])                                               

    ciclista_ds.show()
    ruta_ds.show()
    actividad_ds.show()

    actual_ds = unir_dataframes(ciclista_ds, ruta_ds, actividad_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (307530951, 'Marcia Alfaro','Heredia', None, None, None, None, None,None),
            (201471234, 'Julio Mora','Alajuela', 1, 201471234, '2021-02-15', 1, 'Grecia-Bosque del niño',15),
        ],
        ['cedula', 'nombre_Completo','provincia','codigo_Ruta', 'cedula_Ciclista','fecha','codigo', 'nombre_Ruta','kilometros'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()

def test_union_ciclista_ejecuta_misma_ruta_varias_veces_al_dia(spark_session):
    ciclista_data = [(201471234, 'Julio Mora','Alajuela'), (307530951, 'Marcia Alfaro','Heredia')]
    ciclista_ds = spark_session.createDataFrame(ciclista_data,
                                              ['cedula', 'nombre_Completo','provincia'])
    ruta_data = [(1, 'Grecia-Bosque del niño',15), (2, 'Vuelta a Heredia',50)]
    ruta_ds = spark_session.createDataFrame(ruta_data,
                                               ['codigo', 'nombre_Ruta','kilometros'])
    actividad_data = [(1, 201471234, '2021-02-15'), (2, 307530951, '2021-03-01'), (2, 307530951, '2021-03-01')]
    actividad_ds = spark_session.createDataFrame(actividad_data,
                                              ['codigo_Ruta', 'cedula_Ciclista','fecha'])                                               

    ciclista_ds.show()
    ruta_ds.show()
    actividad_ds.show()

    actual_ds = unir_dataframes(ciclista_ds, ruta_ds, actividad_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (201471234, 'Julio Mora','Alajuela', 1, 201471234, '2021-02-15', 1, 'Grecia-Bosque del niño',15),
            (307530951, 'Marcia Alfaro','Heredia', 2, 307530951, '2021-03-01', 2, 'Vuelta a Heredia',50),
            (307530951, 'Marcia Alfaro','Heredia', 2, 307530951, '2021-03-01', 2, 'Vuelta a Heredia',50),
        ],
        ['cedula', 'nombre_Completo','provincia','codigo_Ruta', 'cedula_Ciclista','fecha','codigo', 'nombre_Ruta','kilometros'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()

def test_union_ruta_no_tiene_actividad(spark_session):
    ciclista_data = [(201471234, 'Julio Mora','Alajuela')]
    ciclista_ds = spark_session.createDataFrame(ciclista_data,
                                              ['cedula', 'nombre_Completo','provincia'])
    ruta_data = [(1, 'Grecia-Bosque del niño',15), (2, 'Vuelta a Heredia',50)]
    ruta_ds = spark_session.createDataFrame(ruta_data,
                                               ['codigo', 'nombre_Ruta','kilometros'])
    actividad_data = [(1, 201471234, '2021-02-15')]
    actividad_ds = spark_session.createDataFrame(actividad_data,
                                              ['codigo_Ruta', 'cedula_Ciclista','fecha'])                                               

    ciclista_ds.show()
    ruta_ds.show()
    actividad_ds.show()

    actual_ds = unir_dataframes(ciclista_ds, ruta_ds, actividad_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (201471234, 'Julio Mora','Alajuela', 1, 201471234, '2021-02-15', 1, 'Grecia-Bosque del niño',15),
        ],
        ['cedula', 'nombre_Completo','provincia','codigo_Ruta', 'cedula_Ciclista','fecha','codigo', 'nombre_Ruta','kilometros'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()