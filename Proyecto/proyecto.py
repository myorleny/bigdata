from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType, FloatType, StructField,
                               StructType, TimestampType, StringType, DateType)
from pyspark.sql.functions import col, date_format, udf, rank, lit      
                             
import proyecto_funciones

import sys

spark = SparkSession \
    .builder \
    .appName("Proyecto Big Data") \
    .config("spark.driver.extraClassPath", 'postgresql-42.2.14.jar') \
    .config("spark.executor.extraClassPath", 'postgresql-42.2.14.jar') \
    .config("spark.jars", 'postgresql-42.2.14.jar') \
    .getOrCreate()
spark.sparkContext.setLogLevel('WARN')


def programaPrincipal():

    #llama a la función que carga los archivos .csv en dataframes
    escuelas_df, ids_df = proyecto_funciones.cargar_archivos_csv()
    
    #almacena en base de datos, en una tabla llamada "escuelas" el dataframe que contiene la información de las escuelas ya procesada
    # escuelas_df \
    #     .write \
    #     .mode("overwrite") \
    #     .format("jdbc") \
    #     .option("url", "jdbc:postgresql://host.docker.internal:5433/postgres") \
    #     .option("user", "postgres") \
    #     .option("password", "testPassword") \
    #     .option("dbtable", "escuelas") \
    #     .option("driver", "org.postgresql.Driver") \
    #     .save()     

    escuelas_df \
        .write \
        .mode("overwrite") \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://host.docker.internal:5433/postgres") \
        .option("user", "postgres") \
        .option("password", "testPassword") \
        .option("dbtable", "escuelas") \
        .save()      

    #almacena en base de datos, en una tabla llamada "indice_desarrollo_social" el dataframe que contiene la información del indice de desarrollo social distrital, ya procesada
    # ids_df \
    #     .write \
    #     .mode("overwrite") \
    #     .format("jdbc") \
    #     .option("url", "jdbc:postgresql://host.docker.internal:5433/postgres") \
    #     .option("user", "postgres") \
    #     .option("password", "testPassword") \
    #     .option("dbtable", "indice_desarrollo_social") \
    #     .option("driver", "org.postgresql.Driver") \
    #     .save()   

    ids_df \
        .write \
        .mode("overwrite") \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://host.docker.internal:5433/postgres") \
        .option("user", "postgres") \
        .option("password", "testPassword") \
        .option("dbtable", "indice_desarrollo_social") \
        .save()           

    # Ejecuta todas las funciones para el preprocesamiento de los datos 
    escuelas_df = proyecto_funciones.excluir_escuelas_sin_matricula(escuelas_df)
    escuelas_df = proyecto_funciones.aplicar_imputacion_valor_fijo(escuelas_df)
    escuelas_df = proyecto_funciones.aplicar_imputacion_con_la_media(escuelas_df)
    escuelas_df = proyecto_funciones.corregir_columnas_negativas(escuelas_df)
    escuelas_df = proyecto_funciones.aplicar_imputacion_aprobados(escuelas_df)
    escuelas_df = proyecto_funciones.agregar_columna_PromocionAlta(escuelas_df)
    # ids_df = proyecto_funciones.reemplazar_nombre_columna (ids_df, 'Codigo', 'CodigoDistrito')
    escuelas_ids_df = proyecto_funciones.join_dataframes(escuelas_df, ids_df)       

    #almacena en base de datos, en una tabla llamada "Escuelas_IDS" el dataframe que contiene la información de las escuelas y el indice de desarrollo social distrital, ya cruzados
    # escuelas_ids_df \
    #     .write \
    #     .mode("overwrite") \
    #     .format("jdbc") \
    #     .option("url", "jdbc:postgresql://host.docker.internal:5433/postgres") \
    #     .option("user", "postgres") \
    #     .option("password", "testPassword") \
    #     .option("dbtable", "Escuelas_IDS") \
    #     .option("driver", "org.postgresql.Driver") \
    #     .save()       

    escuelas_ids_df \
        .write \
        .mode("overwrite") \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://host.docker.internal:5433/postgres") \
        .option("user", "postgres") \
        .option("password", "testPassword") \
        .option("dbtable", "Escuelas_IDS") \
        .save()    

programaPrincipal()