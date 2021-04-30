from .proyecto_funciones import cargar_archivos_csv
from .proyecto_funciones import excluir_escuelas_sin_matricula
from .proyecto_funciones import aplicar_imputacion_valor_fijo
from .proyecto_funciones import aplicar_imputacion_con_la_media
from .proyecto_funciones import corregir_columnas_negativas
from .proyecto_funciones import aplicar_imputacion_aprobados
from .proyecto_funciones import agregar_columna_PromocionAlta
from .proyecto_funciones import join_dataframes

################################# cargar_archivos_csv ###########################################

# Prueba la función cargar_archivos_csv al cargar el archivo de escuelas "megabaseprimaria_2015.csv", valida que al cargar el archivo a un dataframe la cantidad de registros 
# del dataframe coincida con la cantidad real de registros que tiene el csv
def test_cargar_archivos_csv_escuelas(spark_session):
    #llama a la función que carga los archivos .csv en dataframes
    escuelas_df, ids_df = cargar_archivos_csv()
    actual = escuelas_df.count()

    # 4266 es la cantidad de registros que tiene el csv "megabaseprimaria_2015.csv" 
    esperado = 4266

    assert actual == esperado    

# Prueba la función cargar_archivos_csv al cargar el archivo de índice de desarrollo social "IDS_distrital_cantonal.csv", valida que al cargar el archivo a un dataframe la cantidad de registros 
# del dataframe coincida con la cantidad real de registros que tiene el csv
def test_cargar_archivos_csv_ids(spark_session):
    #llama a la función que carga los archivos .csv en dataframes
    escuelas_df, ids_df = cargar_archivos_csv()
    actual = ids_df.count()

    # 477 es la cantidad de registros que tiene el csv "IDS_distrital_cantonal.csv" 
    esperado = 477

    assert actual == esperado       

################################# excluir_escuelas_sin_matricula ###########################################

# Pruebas para la función excluir_escuelas_sin_matricula
# Se prueba la función excluir_escuelas_sin_matricula cuando alguno de los registros de la columna "mit_15" (que corresponde a la matrícula) tiene null, en este caso se espera que ese registro de escuela sea excluido
def test_excluir_escuelas_sin_matricula_campo_matricula_null(spark_session):
    escuelas_data = [(76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 84, 44, 5, 4, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                     (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10111, 1, 1, None, 43, 0, 0, 84, 44, 5, 4, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    ] 
                                    
    escuelas_data_ds = spark_session.createDataFrame(escuelas_data,
                        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])
                                                

    escuelas_data_ds.show()

    actual_ds = excluir_escuelas_sin_matricula(escuelas_data_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 84, 44, 5, 4, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
        ],
        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect() 

# Se prueba la función excluir_escuelas_sin_matricula cuando los campos de todas las columnas están completos, es decir, ninguno tiene null, 
# en este caso se espera que todos los registros sean tomados en cuenta
def test_excluir_escuelas_sin_matricula_todos_los_campos_sin_null(spark_session):
    escuelas_data = [(76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 84, 44, 5, 4, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                     (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10111, 1, 1, 100, 43, 0, 0, 84, 44, 5, 4, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    ] 
                                    
    escuelas_data_ds = spark_session.createDataFrame(escuelas_data,
                        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])
                                                

    escuelas_data_ds.show()

    actual_ds = excluir_escuelas_sin_matricula(escuelas_data_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 84, 44, 5, 4, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
            (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10111, 1, 1, 100, 43, 0, 0, 84, 44, 5, 4, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
        ],
        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()   

# Se prueba la función excluir_escuelas_sin_matricula cuando alguno de los registros tiene null en otras columnas, pero en la columna "mit_15" (que corresponde a la matrícula) sí tiene un valor válido, 
# en este caso se espera que ese registro de escuela NO sea excluido, dado que solo se excluyen los que tienen "mit_15" en null
def test_excluir_escuelas_sin_matricula_otros_campos_null(spark_session):
    escuelas_data = [(76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 84, 44, 5, 4, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                     (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10111, 1, 1, 100, 43, 0, None, 84, 44, 5, None, 0, None, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    ] 
                                    
    escuelas_data_ds = spark_session.createDataFrame(escuelas_data,
                        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])
                                                

    escuelas_data_ds.show()

    actual_ds = excluir_escuelas_sin_matricula(escuelas_data_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 84, 44, 5, 4, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
            (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10111, 1, 1, 100, 43, 0, None, 84, 44, 5, None, 0, None, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
        ],
        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()       

############################# aplicar_imputacion_valor_fijo ###########################################

# Se prueba la función aplicar_imputacion_valor_fijo cuando existen registros con null en varias columnas para las cuales se definió que la imputación debería hacerle sustituyendo el valor 
# null por un cero. En este caso se espera que la función devuelva los mismos registros, pero haciendo el reemplazo del null por 0
def test_aplicar_imputacion_valor_fijo_reemplazo_con_cero(spark_session):
    escuelas_data = [(76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 84, 44, 5, 4, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                     (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10111, 1, 1, 100, 43, 0, None, 84, 44, 5, None, 0, None, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    ] 
                                    
    escuelas_data_ds = spark_session.createDataFrame(escuelas_data,
                        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])
                                                

    escuelas_data_ds.show()

    actual_ds = aplicar_imputacion_valor_fijo(escuelas_data_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 84, 44, 5, 4, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
            (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10111, 1, 1, 100, 43, 0, 0, 84, 44, 5, 0, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
        ],
        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()   

# Se prueba la función aplicar_imputacion_valor_fijo cuando existen registros con null en la columna "creacion00" (que corresponde al año de creación de la escuela)
# En este caso se espera que la función devuelva los mismos registros, pero haciendo el reemplazo del null en la columna "creacion00" por 2015 (que es el año del estudio)
def test_aplicar_imputacion_valor_fijo_reemplaza_AnioCreacion_con_2015(spark_session):
    escuelas_data = [(76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 84, 44, 5, 4, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                     (80, 'VITALIA MADRIGAL ARAYA', None, 1, 1, 101, 10111, 1, 1, 100, 43, 0, 0, 84, 44, 5, 0, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                     (82, 'ANGLO AMERICANA', None, 1, 1, 101, 10112, 1, 1, 500, 250, 0, 0, 450, 200, 5, 0, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    ] 
                                    
    escuelas_data_ds = spark_session.createDataFrame(escuelas_data,
                        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])
                                                

    escuelas_data_ds.show()

    actual_ds = aplicar_imputacion_valor_fijo(escuelas_data_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 84, 44, 5, 4, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
            (80, 'VITALIA MADRIGAL ARAYA', 2015, 1, 1, 101, 10111, 1, 1, 100, 43, 0, 0, 84, 44, 5, 0, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
            (82, 'ANGLO AMERICANA', 2015, 1, 1, 101, 10112, 1, 1, 500, 250, 0, 0, 450, 200, 5, 0, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
        ],
        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()     

# Se prueba la función aplicar_imputacion_valor_fijo cuando los campos de todas las columnas están completos, es decir, ninguno tiene null, 
# en este caso se espera que la función no aplique ningún cambio al dataframe y devuelva los mismos datos
def test_aplicar_imputacion_valor_fijo_todos_los_campos_sin_null(spark_session):
    escuelas_data = [(76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 84, 44, 5, 4, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                     (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10111, 1, 1, 100, 43, 0, 0, 84, 44, 5, 4, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    ] 
                                    
    escuelas_data_ds = spark_session.createDataFrame(escuelas_data,
                        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])
                                                

    escuelas_data_ds.show()

    actual_ds = aplicar_imputacion_valor_fijo(escuelas_data_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 84, 44, 5, 4, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
            (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10111, 1, 1, 100, 43, 0, 0, 84, 44, 5, 4, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
        ],
        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()    

########################### aplicar_imputacion_con_la_media ######################################

# Se prueba la función aplicar_imputacion_con_la_media cuando existen registros con null en las columnas "aat15" (que corresponde a "Aulas para impartir lecciones I y II ciclos total") 
# o "aab15" (que corresponde a "Aulas para impartir lecciones I y II ciclos buenas")
# En este caso se espera que la función devuelva los mismos registros, pero haciendo el reemplazo del valor null por la media de la columna (redondeada, dado que el campo es un integer) 
def test_aplicar_imputacion_con_la_media_columnas_aat15_y_aab15_con_null(spark_session):
    escuelas_data = [(76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 84, 44, 5, 4, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10111, 1, 1, 100, 43, 0, 0, 84, 44, 5, 0, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 7, 7, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (82, 'ANGLO AMERICANA', 1949, 1, 1, 101, 10112, 1, 1, 500, 250, 0, 0, 450, 200, 5, 0, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 30, 30, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (90, 'LAS LETRAS', 1992, 1, 1, 101, 10112, 1, 1, 500, 250, 0, 0, 450, 200, 5, 0, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, None, None, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    ] 
                                    
    escuelas_data_ds = spark_session.createDataFrame(escuelas_data,
                        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])
                                                

    escuelas_data_ds.show()

    actual_ds = aplicar_imputacion_con_la_media(escuelas_data_ds)

    esperado_ds = spark_session.createDataFrame(
        [
 (76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 84, 44, 5, 4, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10111, 1, 1, 100, 43, 0, 0, 84, 44, 5, 0, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 7, 7, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (82, 'ANGLO AMERICANA', 1949, 1, 1, 101, 10112, 1, 1, 500, 250, 0, 0, 450, 200, 5, 0, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 30, 30, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (90, 'LAS LETRAS', 1992, 1, 1, 101, 10112, 1, 1, 500, 250, 0, 0, 450, 200, 5, 0, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 14, 14, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
        ],
        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()  

# Se prueba la función aplicar_imputacion_con_la_media cuando los campos de las columnas "aat15" y "aab15" están completos, es decir, ninguno tiene null, 
# en este caso se espera que la función no aplique ningún cambio al dataframe y devuelva los mismos datos
def test_aplicar_imputacion_con_la_media_todos_los_campos_sin_null(spark_session):
    escuelas_data = [(76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 84, 44, 5, 4, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10111, 1, 1, 100, 43, 0, 0, 84, 44, 5, 0, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 7, 7, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (82, 'ANGLO AMERICANA', 1949, 1, 1, 101, 10112, 1, 1, 500, 250, 0, 0, 450, 200, 5, 0, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 30, 30, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (90, 'LAS LETRAS', 1992, 1, 1, 101, 10112, 1, 1, 500, 250, 0, 0, 450, 200, 5, 0, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 30, 30, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    ] 
                                    
    escuelas_data_ds = spark_session.createDataFrame(escuelas_data,
                        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])
                                                

    escuelas_data_ds.show()

    actual_ds = aplicar_imputacion_con_la_media(escuelas_data_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 84, 44, 5, 4, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10111, 1, 1, 100, 43, 0, 0, 84, 44, 5, 0, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 7, 7, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (82, 'ANGLO AMERICANA', 1949, 1, 1, 101, 10112, 1, 1, 500, 250, 0, 0, 450, 200, 5, 0, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 30, 30, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (90, 'LAS LETRAS', 1992, 1, 1, 101, 10112, 1, 1, 500, 250, 0, 0, 450, 200, 5, 0, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 30, 30, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
        ],
        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()    

########################### corregir_columnas_negativas ######################################

# Se prueba la función corregir_columnas_negativas cuando existen registros negativos en las columnas 'desert_15' o 'deserh_15'
# En este caso se espera que la función devuelva los mismos registros, pero haciendo el reemplazo del valor negativo por un 0
def test_corregir_columnas_negativas_desert15_y_deserh15(spark_session):
    escuelas_data = [(76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 84, 44, 5, 4, 0, 0, -3, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10111, 1, 1, 100, 43, 0, 0, 84, 44, 5, 0, 0, 0, -2, -1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 7, 7, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (82, 'ANGLO AMERICANA', 1949, 1, 1, 101, 10112, 1, 1, 500, 250, 0, 0, 450, 200, 5, 0, 0, 0, -10, -5, 0, 0, 11, 6, 1, 0, 0, 0, 0, 30, 30, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    ] 
                                    
    escuelas_data_ds = spark_session.createDataFrame(escuelas_data,
                        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])
                                                

    escuelas_data_ds.show()

    actual_ds = corregir_columnas_negativas(escuelas_data_ds)

    esperado_ds = spark_session.createDataFrame(
        [
                    (76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 84, 44, 5, 4, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10111, 1, 1, 100, 43, 0, 0, 84, 44, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 7, 7, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (82, 'ANGLO AMERICANA', 1949, 1, 1, 101, 10112, 1, 1, 500, 250, 0, 0, 450, 200, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 30, 30, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
        ],
        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()  

# Se prueba la función corregir_columnas_negativas cuando los campos de las columnas desert_15 y deserh_15 están completos, es decir, ninguno tiene null, 
# en este caso se espera que la función no aplique ningún cambio al dataframe y devuelva los mismos datos
def test_corregir_columnas_negativas_columnas_desert15_y_deserh15_sin_nulos(spark_session):
    escuelas_data = [(76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 84, 44, 5, 4, 0, 0, 3, 1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10111, 1, 1, 100, 43, 0, 0, 84, 44, 5, 0, 0, 0, 2, 1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 7, 7, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (82, 'ANGLO AMERICANA', 1949, 1, 1, 101, 10112, 1, 1, 500, 250, 0, 0, 450, 200, 5, 0, 0, 0, 10, 5, 0, 0, 11, 6, 1, 0, 0, 0, 0, 30, 30, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    ] 
                                    
    escuelas_data_ds = spark_session.createDataFrame(escuelas_data,
                        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])
                                                

    escuelas_data_ds.show()

    actual_ds = corregir_columnas_negativas(escuelas_data_ds)

    esperado_ds = spark_session.createDataFrame(
        [
                    (76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 84, 44, 5, 4, 0, 0, 3, 1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10111, 1, 1, 100, 43, 0, 0, 84, 44, 5, 0, 0, 0, 2, 1, 0, 0, 11, 6, 1, 0, 0, 0, 0, 7, 7, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (82, 'ANGLO AMERICANA', 1949, 1, 1, 101, 10112, 1, 1, 500, 250, 0, 0, 450, 200, 5, 0, 0, 0, 10, 5, 0, 0, 11, 6, 1, 0, 0, 0, 0, 30, 30, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),

        ],
        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()                

########################### aplicar_imputacion_aprobados ######################################

# Se prueba la función aplicar_imputacion_aprobados cuando existen registros en la columna 'aprobt_15' (cantidad de aprobados total) 
# cuyo valor es mayor a la cantidad total de estudiantes matriculados (es decir, es un valor inconsistente) 
# En este caso se espera que se reemplacen dichos valores con la siguiente fórmula: aprobt_15 = mit_15 - reprot_15 - desa_15 - desert_15
# (cantidad de aprobados total = cantidad de matriculados total - cantidad de reprobados total - cantidad de abandono total - cantidad con exclusión intra-anual total)
def test_aplicar_imputacion_aprobados_con_valores_mayores_a_matriculados_columna_aprobt15(spark_session):
    escuelas_data = [(76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 80, 40, 5, 4, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10111, 1, 1, 100, 43, 0, 0, 114, 40, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 7, 7, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (82, 'ANGLO AMERICANA', 1949, 1, 1, 101, 10112, 1, 1, 500, 250, 0, 0, 600, 200, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 30, 30, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    ] 
                                    
    escuelas_data_ds = spark_session.createDataFrame(escuelas_data,
                        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])
                                                

    escuelas_data_ds.show()

    actual_ds = aplicar_imputacion_aprobados(escuelas_data_ds)

    esperado_ds = spark_session.createDataFrame(
        [
                    (76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 80, 40, 5, 4, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10111, 1, 1, 100, 43, 0, 0, 95, 40, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 7, 7, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (82, 'ANGLO AMERICANA', 1949, 1, 1, 101, 10112, 1, 1, 500, 250, 0, 0, 495, 200, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 30, 30, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
        ],
        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()  

# Se prueba la función aplicar_imputacion_aprobados cuando existen registros en la columna 'aprobh_15' (cantidad de aprobados hombres)
# cuyo valor es mayor a la cantidad total de estudiantes matriculados hombres (es decir, es un valor inconsistente) 
# En este caso se espera que se reemplacen dichos valores con la siguiente fórmula: aprobh_15 = mih_15 - reproh_15 - desah_15 - deserh_15
# (cantidad de aprobados hombres = cantidad de matriculados hombres - cantidad de reprobados hombres - cantidad de abandono hombres - cantidad con exclusión intra-anual hombres)
def test_aplicar_imputacion_aprobados_con_valores_mayores_a_matriculados_columna_aprobh15(spark_session):
    escuelas_data = [(76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 80, 40, 5, 4, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10111, 1, 1, 100, 43, 0, 0, 95, 50, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 7, 7, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (82, 'ANGLO AMERICANA', 1949, 1, 1, 101, 10112, 1, 1, 500, 250, 0, 0, 495, 450, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 30, 30, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    ] 
                                    
    escuelas_data_ds = spark_session.createDataFrame(escuelas_data,
                        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])
                                                

    escuelas_data_ds.show()

    actual_ds = aplicar_imputacion_aprobados(escuelas_data_ds)

    esperado_ds = spark_session.createDataFrame(
        [
                    (76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 80, 40, 5, 4, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10111, 1, 1, 100, 43, 0, 0, 95, 43, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 7, 7, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (82, 'ANGLO AMERICANA', 1949, 1, 1, 101, 10112, 1, 1, 500, 250, 0, 0, 495, 250, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 30, 30, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
        ],
        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()      

# Se prueba la función aplicar_imputacion_aprobados cuando existen registros en la columna 'aprobt_15' (cantidad de aprobados total) 
# cuyo valor es null 
# En este caso se espera que se reemplacen los valores nulos con la siguiente fórmula: aprobt_15 = mit_15 - reprot_15 - desa_15 - desert_15
# (cantidad de aprobados total = cantidad de matriculados total - cantidad de reprobados total - cantidad de abandono total - cantidad con exclusión intra-anual total)
def test_aplicar_imputacion_aprobados_columna_aprobt15_null(spark_session):
    escuelas_data = [(76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 80, 40, 5, 4, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10111, 1, 1, 100, 43, 0, 0, None, 40, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 7, 7, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (82, 'ANGLO AMERICANA', 1949, 1, 1, 101, 10112, 1, 1, 500, 250, 0, 0, None, 200, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 30, 30, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    ] 
                                    
    escuelas_data_ds = spark_session.createDataFrame(escuelas_data,
                        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])
                                                

    escuelas_data_ds.show()

    actual_ds = aplicar_imputacion_aprobados(escuelas_data_ds)

    esperado_ds = spark_session.createDataFrame(
        [
                    (76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 80, 40, 5, 4, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10111, 1, 1, 100, 43, 0, 0, 95, 40, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 7, 7, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (82, 'ANGLO AMERICANA', 1949, 1, 1, 101, 10112, 1, 1, 500, 250, 0, 0, 495, 200, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 30, 30, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
        ],
        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()  

# Se prueba la función aplicar_imputacion_aprobados cuando existen registros en la columna 'aprobh_15' (cantidad de aprobados hombres)
# cuyo valor es null  
# En este caso se espera que se reemplacen los valores nulos con la siguiente fórmula: aprobh_15 = mih_15 - reproh_15 - desah_15 - deserh_15
# (cantidad de aprobados hombres = cantidad de matriculados hombres - cantidad de reprobados hombres - cantidad de abandono hombres - cantidad con exclusión intra-anual hombres)
def test_aplicar_imputacion_aprobados_columna_aprobh15_null(spark_session):
    escuelas_data = [(76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 80, 40, 5, 4, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10111, 1, 1, 100, 43, 0, 0, 95, None, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 7, 7, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (82, 'ANGLO AMERICANA', 1949, 1, 1, 101, 10112, 1, 1, 500, 250, 0, 0, 495, None, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 30, 30, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    ] 
                                    
    escuelas_data_ds = spark_session.createDataFrame(escuelas_data,
                        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])
                                                

    escuelas_data_ds.show()

    actual_ds = aplicar_imputacion_aprobados(escuelas_data_ds)

    esperado_ds = spark_session.createDataFrame(
        [
                    (76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 80, 40, 5, 4, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10111, 1, 1, 100, 43, 0, 0, 95, 43, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 7, 7, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (82, 'ANGLO AMERICANA', 1949, 1, 1, 101, 10112, 1, 1, 500, 250, 0, 0, 495, 250, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 30, 30, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
        ],
        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()    

# Se prueba la función aplicar_imputacion_aprobados cuando los campos de las columnas aprobt_15 y aprobh_15 están correctos, es decir, ninguno tiene valores mayores a los matriculados o null, 
# en este caso se espera que la función no aplique ningún cambio al dataframe y devuelva los mismos datos
def test_aplicar_imputacion_aprobados_columnas_aprobt15_y_aprobh15_correctas(spark_session):
    escuelas_data = [(76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 80, 40, 5, 4, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10111, 1, 1, 100, 43, 0, 0, 95, 43, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 7, 7, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (82, 'ANGLO AMERICANA', 1949, 1, 1, 101, 10112, 1, 1, 500, 250, 0, 0, 495, 250, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 30, 30, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    ] 
                                    
    escuelas_data_ds = spark_session.createDataFrame(escuelas_data,
                        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])
                                                

    escuelas_data_ds.show()

    actual_ds = aplicar_imputacion_aprobados(escuelas_data_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 80, 40, 5, 4, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
            (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10111, 1, 1, 100, 43, 0, 0, 95, 43, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 7, 7, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
            (82, 'ANGLO AMERICANA', 1949, 1, 1, 101, 10112, 1, 1, 500, 250, 0, 0, 495, 250, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 30, 30, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
],
        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect() 

################################## agregar_columna_PromocionAlta ##########################################
#       
# Se prueba la función agregar_columna_PromocionAlta cuando las escuelas presentan un porcentaje de aprobación (cantidad de aprobados*100/cantidad de matriculados: aprobt_15'*100/'mit_15') 
# mayor al 95%. En este caso se espera que la función devuelva el mismo dataframe pero con una columna adicional llamada "PromocionAlta" que tenga el valor de 1 para cada registro
def test_agregar_columna_PromocionAlta_Si(spark_session):
    escuelas_data = [
                    (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10111, 1, 1, 100, 43, 0, 0, 96, 43, 4, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 7, 7, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (82, 'ANGLO AMERICANA', 1949, 1, 1, 101, 10112, 1, 1, 500, 250, 0, 0, 495, 250, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 30, 30, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    ] 
                                    
    escuelas_data_ds = spark_session.createDataFrame(escuelas_data,
                        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])
                                                

    escuelas_data_ds.show()

    actual_ds = agregar_columna_PromocionAlta(escuelas_data_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10111, 1, 1, 100, 43, 0, 0, 96, 43, 4, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 7, 7, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0, 1),
            (82, 'ANGLO AMERICANA', 1949, 1, 1, 101, 10112, 1, 1, 500, 250, 0, 0, 495, 250, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 30, 30, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0, 1),
        ],
        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15','PromocionAlta'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect() 

# Se prueba la función agregar_columna_PromocionAlta cuando las escuelas presentan un porcentaje de aprobación (cantidad de aprobados*100/cantidad de matriculados: aprobt_15'*100/'mit_15') 
# menor o igual al 95%. En este caso se espera que la función devuelva el mismo dataframe pero con una columna adicional llamada "PromocionAlta" que tenga el valor de 0 para cada registro
def test_agregar_columna_PromocionAlta_No(spark_session):
    escuelas_data = [
                    (76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 80, 40, 5, 4, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10111, 1, 1, 100, 43, 0, 0, 95, 43, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 7, 7, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    ] 
                                    
    escuelas_data_ds = spark_session.createDataFrame(escuelas_data,
                        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])
                                                

    escuelas_data_ds.show()

    actual_ds = agregar_columna_PromocionAlta(escuelas_data_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 80, 40, 5, 4, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0, 0),
            (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10111, 1, 1, 100, 43, 0, 0, 95, 43, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 7, 7, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0, 0),
        ],
        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15','PromocionAlta'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()     

################################# join_dataframes ###########################################

# Se prueba la función join_dataframes cuando todos los campos de la columna cddis15 del dataframe de escuelas (que corresponde al distrito) hace join con algún campo 
# de la columna Codigo del dataset IDS (índice de desarrollo social)
# En este caso se espera que la función devuelve un dataframe con el join de las escuelas con la información del índice de desarrollo social del distrito donde se encuentra la escuela
def test_join_dataframes_datos_coinciden_en_ambos_dataframes(spark_session):
    escuelas_data = [(76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 80, 40, 5, 4, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10101, 1, 1, 100, 43, 0, 0, 95, 43, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 7, 7, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (82, 'ANGLO AMERICANA', 1949, 1, 3,	303, 30305, 1, 1, 500, 250, 0, 0, 495, 250, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 30, 30, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    ] 
                                    
    escuelas_data_ds = spark_session.createDataFrame(escuelas_data,
                        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])
                                                
    escuelas_data_ds.show()

    ids_data = [(10111, 'San Sebastián', 45.9, 35.2, 89.1, 91.3, 77.41),
                (10101, 'Carmen', 68.9, 36.0, 92.5, 86.0, 85.55),
                (30305, 'Concepción', 48.1, 41.7, 67.2, 86.5, 71.6),
               ] 
                                    
    ids_data_ds = spark_session.createDataFrame(ids_data,
                        ['Codigo', 'Distrito', 'Dimension_Economica', 'Dimension_Participacion', 'Dimension_Salud', 'Dimension_Educativa', 'IDS'])
                                                
    ids_data_ds.show()    

    actual_ds = join_dataframes(escuelas_data_ds, ids_data_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 80, 40, 5, 4, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0, 10111, 'San Sebastián', 45.9, 35.2, 89.1, 91.3, 77.41),
            (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10101, 1, 1, 100, 43, 0, 0, 95, 43, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 7, 7, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0, 10101, 'Carmen', 68.9, 36.0, 92.5, 86.0, 85.55),
            (82, 'ANGLO AMERICANA', 1949, 1, 3,	303, 30305, 1, 1, 500, 250, 0, 0, 495, 250, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 30, 30, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0, 30305, 'Concepción', 48.1, 41.7, 67.2, 86.5, 71.6),
        ],
        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
         'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15', 'Codigo', 'Distrito', 'Dimension_Economica', 'Dimension_Participacion', 'Dimension_Salud', 'Dimension_Educativa', 'IDS'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()       

# Se prueba la función join_dataframes cuando hay escuelas registradas cuyo distrito no tiene el índice de desarrollo social registrado en la dataset de IDS 
# En este caso se espera que la función NO devuelva esa escuela en el join 
def test_join_dataframes_distrito_de_escuela_no_tiene_ids_registrado(spark_session):
    escuelas_data = [(76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 80, 40, 5, 4, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10101, 1, 1, 100, 43, 0, 0, 95, 43, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 7, 7, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (82, 'ANGLO AMERICANA', 1949, 1, 3,	303, 30305, 1, 1, 500, 250, 0, 0, 495, 250, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 30, 30, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    ] 
                                    
    escuelas_data_ds = spark_session.createDataFrame(escuelas_data,
                        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])
                                                
    escuelas_data_ds.show()

    ids_data = [(10111, 'San Sebastián', 45.9, 35.2, 89.1, 91.3, 77.41),
                (10101, 'Carmen', 68.9, 36.0, 92.5, 86.0, 85.55),
               ] 
                                    
    ids_data_ds = spark_session.createDataFrame(ids_data,
                        ['Codigo', 'Distrito', 'Dimension_Economica', 'Dimension_Participacion', 'Dimension_Salud', 'Dimension_Educativa', 'IDS'])
                                                
    ids_data_ds.show()    

    actual_ds = join_dataframes(escuelas_data_ds, ids_data_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 80, 40, 5, 4, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0, 10111, 'San Sebastián', 45.9, 35.2, 89.1, 91.3, 77.41),
            (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10101, 1, 1, 100, 43, 0, 0, 95, 43, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 7, 7, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0, 10101, 'Carmen', 68.9, 36.0, 92.5, 86.0, 85.55),
        ],
        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
         'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15', 'Codigo', 'Distrito', 'Dimension_Economica', 'Dimension_Participacion', 'Dimension_Salud', 'Dimension_Educativa', 'IDS'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()  

# Se prueba la función join_dataframes cuando hay distritos en el dataset de IDS (índice de desarrollo social) en los cuales no se tienen escuelas registradas
# En este caso se espera que la función NO devuelva ese registro en el join 
def test_join_dataframes_distrito_de_ids_no_tiene_escuelas_registradas(spark_session):
    escuelas_data = [(76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 80, 40, 5, 4, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10101, 1, 1, 100, 43, 0, 0, 95, 43, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 7, 7, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    (82, 'ANGLO AMERICANA', 1949, 1, 3,	303, 30305, 1, 1, 500, 250, 0, 0, 495, 250, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 30, 30, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0),
                    ] 
                                    
    escuelas_data_ds = spark_session.createDataFrame(escuelas_data,
                        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
                        'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15'])
                                                
    escuelas_data_ds.show()

    ids_data = [(10111, 'San Sebastián', 45.9, 35.2, 89.1, 91.3, 77.41),
                (10101, 'Carmen', 68.9, 36.0, 92.5, 86.0, 85.55),
                (30305, 'Concepción', 48.1, 41.7, 67.2, 86.5, 71.6),
                (60506, 'Bahía Drake', 9.3, 38.0, 61.4, 36.6, 44.5)
               ] 
                                    
    ids_data_ds = spark_session.createDataFrame(ids_data,
                        ['Codigo', 'Distrito', 'Dimension_Economica', 'Dimension_Participacion', 'Dimension_Salud', 'Dimension_Educativa', 'IDS'])
                                                
    ids_data_ds.show()    

    actual_ds = join_dataframes(escuelas_data_ds, ids_data_ds)

    esperado_ds = spark_session.createDataFrame(
        [
            (76, 'VIRGEN DE GUADALUPE', 1990, 1, 1, 101, 10111, 1, 1, 85, 43, 0, 0, 80, 40, 5, 4, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 6, 6, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0, 10111, 'San Sebastián', 45.9, 35.2, 89.1, 91.3, 77.41),
            (80, 'VITALIA MADRIGAL ARAYA', 1908, 1, 1, 101, 10101, 1, 1, 100, 43, 0, 0, 95, 43, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 7, 7, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0, 10101, 'Carmen', 68.9, 36.0, 92.5, 86.0, 85.55),
            (82, 'ANGLO AMERICANA', 1949, 1, 3,	303, 30305, 1, 1, 500, 250, 0, 0, 495, 250, 5, 0, 0, 0, 0, 0, 0, 0, 11, 6, 1, 0, 0, 0, 0, 30, 30, 2, 2, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 6, 6, 0, 0, 1, 1, 1, 1, 7, 7, 0, 0, 0, 23, 0, 21, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 99, 99, 1, 1, 1, 0, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 48, 0, 0, 85, 48, 0, 0, 4, 0, 30305, 'Concepción', 48.1, 41.7, 67.2, 86.5, 71.6),
        ],
        ['llave', 'nombre_ins', 'creacion00', 'direg15', 'cdpr15', 'cdcan15', 'cddis15', 'regplan15', 'zona15', 'mit_15', 'mih_15', 'rt_15', 'rh_15', 'aprobt_15', 'aprobh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'aat15', 'aab15', 'apt15', 'apb15', 'aest15', 'aesb15', 'aaet15', 'aaeb15', 'anat15', 'anab15', 'inft15', 'infb15', 'olat15', 'olab15', 'salt15', 'salb15', 'comt15', 'comb15', 'bibt15', 'bibb15', 'gimt15', 'gimb15', 'talt_ai15', 'talb_ai15', 'otalt15', 'otalb15', 'sodt15', 'sodb15', 'indt15', 'indb15', 'lavt15', 'lavb15', 'sant15', 'sanb15', 'tvt15', 'tvb15', 'vbt15', 'vbb15', 'dvdt15', 'dvdb15', 'cetoi15', 'cetos15', 'cepei15', 'cepes15', 'cepai15', 'cepas15', 'ceadi15', 'ceads15', 'cptoi15', 'cptos15', 'cppei15', 'cppes15', 'cppai15', 'cppas15', 'cpadi15', 'cpads15', 'bib15', 'sal15', 'pla15', 'aux15', 'serv_int15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 
         'agrvep_15', 'agrveo_15', 'agrfe_15', 'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15', 'agrde_15', 'agrdep_15', 'agrdeo_15', 'agroe_15', 'agroep_15', 'agroeo_15', 'int_15', 'inht_15', 'rit_15', 'rih_15', 'frt_15', 'frh_15', 'itt_15', 'ith_15', 'extrant_15', 'extranh_15', 'Codigo', 'Distrito', 'Dimension_Economica', 'Dimension_Participacion', 'Dimension_Salud', 'Dimension_Educativa', 'IDS'])

    esperado_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == esperado_ds.collect()                




