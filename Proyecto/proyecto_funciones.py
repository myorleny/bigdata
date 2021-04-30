
from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType, FloatType, StructField,
                               StructType, TimestampType, StringType, DateType)
from pyspark.sql.functions import col, date_format, udf, rank, lit, mean, round, when                          
from pyspark.sql.window import Window
import matplotlib.pyplot as plt

spark = SparkSession \
    .builder \
    .appName("Proyecto Big Data") \
    .config("spark.driver.extraClassPath", 'postgresql-42.2.14.jar') \
    .config("spark.executor.extraClassPath", 'postgresql-42.2.14.jar') \
    .config("spark.jars", 'postgresql-42.2.14.jar') \
    .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

def cargar_archivos_csv():
    # Se carga a un dataframe el archivo csv que contiene la información de las escuelas
    escuelas_df = spark \
        .read \
        .format("csv") \
        .option("path", "./Fuentes_de_datos/megabaseprimaria_2015.csv") \
        .option("header", True) \
        .option("inferschema", "true") \
        .option("delimiter", ",") \
        .load()

    # Se carga a un dataframe el archivo csv que contiene la información del índice de desarrollo social distrital
    ids_df = spark \
        .read \
        .format("csv") \
        .option("path", "./Fuentes_de_datos/IDS_distrital_cantonal.csv") \
        .option("header", True) \
        .option("inferschema", "true") \
        .option("delimiter", ",") \
        .load()   

    return  escuelas_df, ids_df        

   
# cargar_archivos_csv()

def excluir_escuelas_sin_matricula(escuelas_df):
    # Se excluyen del dataset las escuelas que no tienen información de matrícula. Se toma esta decisión dado que aplicar algún método de imputación 
    # sobre esta columna más bien podría afectar los resultados del modelo de predicción
    escuelas_df = escuelas_df.dropna('any',None,'mit_15')
    
    return escuelas_df

def aplicar_imputacion_valor_fijo(escuelas_df):

    # Lista con los nombres de columnas a las cuales se les hará la imputación de los valores nulos por 0
    # Se decide sustitir por 0 porque estas columnas corresponden a valores como repitentes, reprobados, abandono, exclusión, estudiantes con adecuación, embarazadas,
    # expulsiones, casos atendidos entre estudiantes por diferentes razones, y cantidad de extrangeros. Por lo tanto se asume que si esos valores están en null significa que no hay casos
    lista_columnas_imputar_con_cero = ['rt_15', 'rh_15', 'reprot_15', 'reproh_15', 'desa_15', 'desah_15', 'desert_15', 'deserh_15', 'act_15', 'ach_15', 'nst_15', 'nsh_15', 
    'sit_15', 'sih_15', 'embt_15', 'embmenor_15', 'embmayor_15', 'expto_15', 'expdef_15', 'exptem_15', 'agrve_15', 'agrvep_15', 'agrveo_15', 'agrfe_15', 
    'agrfep_15', 'agrfeo_15', 'agree_15', 'agreep_15', 'agreeo_15', 'agrre_15', 'agrrep_15', 'agrreo_15',	'agrde_15', 'agrdep_15', 'agrdeo_15', 
    'agroe_15', 'agroep_15', 'agroeo_15', 'extrant_15', 'extranh_15']

    # Realiza la imputación de los valores de esas columnas cuando están en null, con cero
    escuelas_df = escuelas_df.fillna(0,lista_columnas_imputar_con_cero)

    # Realiza la imputación en la columna "creacion00" que corresponde al año de creación de la escuela, para que si está en null, le ponga 2015, que es el año del estudio
    escuelas_df = escuelas_df.fillna(2015,'creacion00')

    return escuelas_df

def aplicar_imputacion_con_la_media(escuelas_df):

    # Se realiza imputación con la media para las columnas "aat15" y "aab15"
    # Se decide sustitir por la media porque estas columnas corresponden a cantidad total de aulas y a cantidad de aulas buenas, por lo que se considera que la media 
    # es un valor aceptable para sustituir los nulos

    lista_columnas_imputar_con_media = ['aat15', 'aab15']
    
    for columna in lista_columnas_imputar_con_media:
        # Se obtiene el valor de la media para la columna
        media_df = \
        escuelas_df.select(
            round(mean(col(columna))).alias('media')
        )
        media_df = media_df.select(media_df.media.cast(IntegerType()))
        media = media_df.collect()[0]

        valor_media = media[0]  

        # Se realiza la imputación de los nulos por la media
        escuelas_df = escuelas_df.fillna(valor_media,columna)
    
#    # Se obtiene el valor de la media de la columna aab15
#     media_df = \
#     escuelas_df.select(
#         round(mean(col('aab15'))).alias('media')
#     )
#     media_df = media_df.select(media_df.media.cast(IntegerType()))
#     media = media_df.collect()[0]

#     valor_media = media[0]  

#     print (valor_media)
    
#     # Se realiza la imputación de los nulos por la media
#     escuelas_df = escuelas_df.fillna(valor_media,'aab15')    

    return escuelas_df

def aplicar_imputacion_con_la_moda(escuelas_df):


    bins, counts = escuelas_df.select('regplan15').rdd.flatMap(lambda x: x).histogram(20)
    print(bins, counts)
    # plt.hist(bins[:-1], bins=bins, weights=counts)
    print(plt)
    # plt.show()

    # Se realiza imputación con la media para las columnas "aat15" y "aab15"
    # Se decide sustitir por la media porque estas columnas corresponden a cantidad total de aulas y a cantidad de aulas buenas, por lo que se considera que la media 
    # es un valor aceptable para sustituir los nulos

    # lista_columnas_imputar_con_moda = ['regplan15']
    
    # for columna in lista_columnas_imputar_con_moda:
    #     # Se obtiene el valor de la media para la columna
    #     media_df = \
    #     escuelas_df.select(
    #         round(mode(col(columna))).alias('media')
    #     )
    #     media_df = media_df.select(media_df.media.cast(IntegerType()))
    #     media = media_df.collect()[0]

    #     valor_media = media[0]  

    #     print (valor_media)

    #     # Se realiza la imputación de los nulos por la media
    #     escuelas_df = escuelas_df.fillna(valor_media,columna)
    
   # Se obtiene el valor de la media de la columna aab15
    # media_df = \
    # escuelas_df.select(
    #     round(mode(col('regplan15'))).alias('media')
    # )
    # media_df = media_df.select(media_df.media.cast(IntegerType()))
    # media = media_df.collect()[0]

    # valor_media = media[0]  

    # print (valor_media)
    
    # # Se realiza la imputación de los nulos por la media
    # escuelas_df = escuelas_df.fillna(valor_media,'regplan15')    

    # return escuelas_df    

def corregir_columnas_negativas(escuelas_df):

    # Existen 2 columnas que erroneamente tienen valores negativos ("Exclusión intra-anual Total (desert_15)" y "Exclusión intra-anual Hombres (deserh_15)"), 
    # por lo tanto se toma la desición de setear los valores negativos a 0

    # escuelas_df = escuelas_df.where(col('deserh_15') < 0)
    # print (escuelas_df.count())
    
    valor = 0

    lista_columnas_a_actualizar = ['desert_15', 'deserh_15']

    for columna in lista_columnas_a_actualizar:
        escuelas_df = escuelas_df.withColumn(
            columna,
            when(
                col(columna) < 0,
                valor
            ).otherwise(col(columna))
            )

    # escuelas_df = escuelas_df.where(col('deserh_15') < 0)
    # print (escuelas_df.count())   
    return escuelas_df    

def aplicar_imputacion_aprobados(escuelas_df):

    # En la columna de "cantidad de aprobados total (aprobt_15)" y "cantidad de aprobados hombres (aprobh_15)" existen valores erroneos donde la cantidad
    # de aprobados es mayor a la cantidad de matriculados, o bien, la columna está en null. Para estos casos se procede a calcular la cantida de aprobados
    # como cantidad de matriculas - cantidad de reprobados - cantidad de abandono - cantidad con exclusión intra-anual
    
    # escuelas_df = escuelas_df.where((col('aprobh_15') > col('mih_15')) | (col('aprobh_15').isNull()))
    # print (escuelas_df.count())
    
    # Calculo para la columna "aprobt_15"
    escuelas_df = escuelas_df.withColumn(
        'aprobt_15',
        when(
            (col('aprobt_15') > col('mit_15')) | (col('aprobt_15').isNull()),
            col('mit_15')-col('reprot_15')-col('desa_15')-col('desert_15')
        ).otherwise(col('aprobt_15'))
        )

    # Calculo para la columna "aprobh_15"
    escuelas_df = escuelas_df.withColumn(
        'aprobh_15',
        when(
            (col('aprobh_15') > col('mih_15')) | (col('aprobh_15').isNull()),
            col('mih_15')-col('reproh_15')-col('desah_15')-col('deserh_15')
        ).otherwise(col('aprobh_15'))
        )        

    # escuelas_df = escuelas_df.where(col('aprobt_15') > col('mit_15'))
    # print (escuelas_df.count())   
    # escuelas_df = escuelas_df.where(col('llave') == 160)
    # escuelas_df = escuelas_df.select (col('aprobh_15'))
    # escuelas_df.show()
    return escuelas_df    

def agregar_columna_PromocionAlta(escuelas_df):

    # Se agrega la columna "PromocionAlta" que será nuestro objetivo de predicción binaria    
    # Primero se agrega la columna "PorcentajeAprobados" que se obtiene del cálculo aprobados*100/matriculados
    escuelas_df = escuelas_df.withColumn(
        'PorcentajeAprobados', (col('aprobt_15')*100/col('mit_15')))  
    
    # Ahora se agrega la columna PromocionAlta basada en la columna anterior
    escuelas_df = escuelas_df.withColumn(
        'PromocionAlta',
        when(
            (col('PorcentajeAprobados') > 95),
            1
        ).otherwise(0)
        )        

    # escuelas_df.show()
    escuelas_df = escuelas_df \
        .drop('PorcentajeAprobados') \

    # escuelas_df = escuelas_df.where(col('aprobt_15') > col('mit_15'))
    # print (escuelas_df.count())   
    # escuelas_df = escuelas_df.where(col('PromocionAlta') == 0)
    # escuelas_df = escuelas_df.select (col('PromocionAlta'))
    # escuelas_df.show()
    # print (escuelas_df.count())
    return escuelas_df  

# def verifica_columnas_nulas_en_escuelas(escuelas_df):

#     # Verifica que no haya quedado ninguna columna con valores nulos
#     cantidad_nulos = 0
#     for columna in escuelas_df.columns: 
#         columnas_null_df = escuelas_df.where(col(columna).isNull())
#         if columnas_null_df.count() != 0:
#             cantidad_nulos = cantidad_nulos + columnas_null_df.count()
#             print ("Existen valores nulos en la columna: ", columna)   

#     if  cantidad_nulos == 0:
#         print ("No existe ninguna columna con valores nulos.")      

def reemplazar_nombre_columna (df, nombre_anterior, nombre_nuevo):
    # Renombra columna de un dataset
    df = df.withColumnRenamed(nombre_anterior, nombre_nuevo)
    
    return df

def join_dataframes(escuelas_df, ids_df):
    
    # Une los datos de los 2 dataframes: escuelas y IDS (índice de desarrollo social)
    escuelas_ids_df = escuelas_df.join(ids_df, escuelas_df.cddis15 == ids_df.Codigo)

    return escuelas_ids_df

def program_princ():
    escuelas_df, ids_df = cargar_archivos_csv()
    escuelas_df = excluir_escuelas_sin_matricula(escuelas_df)
    escuelas_df = aplicar_imputacion_valor_fijo(escuelas_df)
    escuelas_df = aplicar_imputacion_con_la_media(escuelas_df)
    escuelas_df = corregir_columnas_negativas(escuelas_df)
    escuelas_df = aplicar_imputacion_aprobados(escuelas_df)
    escuelas_df = agregar_columna_PromocionAlta(escuelas_df)
    # escuelas_df = verifica_columnas_nulas_en_escuelas(escuelas_df)
    # ids_df = reemplazar_nombre_columna (ids_df, 'Codigo', 'CodigoDistrito')
    # escuelas_ids_df = join_dataframes(escuelas_df, ids_df)

program_princ()    



