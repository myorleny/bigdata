from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType, FloatType, StructField,
                               StructType, TimestampType, StringType, DateType)

import tarea1_funciones

import argparse

spark = SparkSession.builder.appName("Actividades de Ciclistas").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

def programaPrincipal():

    parser = argparse.ArgumentParser()
    parser.add_argument("ciclistas", help = "Nombre del archivo csv que contiene la lista de ciclistas")                      
    parser.add_argument("rutas", help = "Nombre del archivo csv que contiene la lista de rutas")
    parser.add_argument("actividades", help = "Nombre del archivo csv que contiene la lista de actividades")
    parser.add_argument("N", help = "Top N a consultar de ciclistas por provincia", type = int)

    args = parser.parse_args()

    ciclista_schema = StructType([StructField('cedula', IntegerType()),
                            StructField('nombre_Completo', StringType()),
                            StructField('provincia', StringType()),
                            ])

    ciclista_df = spark.read.csv(args.ciclistas,
                            schema=ciclista_schema,
                            header=False)

    #ciclista_df.show()

    ruta_schema = StructType([StructField('codigo', IntegerType()),
                            StructField('nombre_Ruta', StringType()),
                            StructField('kilometros', FloatType()),
                            ])

    ruta_df = spark.read.csv(args.rutas,
                            schema=ruta_schema,
                            header=False)

    #ruta_df.show()

    actividad_schema = StructType([StructField('codigo_Ruta', IntegerType()),
                            StructField('cedula_Ciclista', IntegerType()),
                            StructField('fecha', DateType()),
                            ])

    actividad_df = spark.read.csv(args.actividades,
                            schema=actividad_schema,
                            header=False)

    #actividad_df.show()    

    ciclista_actividad_ruta_df = tarea1_funciones.join_dataframes(ciclista_df, ruta_df, actividad_df)

    print("Dataframe que contiene el join de los 3 archivos: ciclista.csv, actividad.csv y ruta.csv:")
    ciclista_actividad_ruta_df.show()

    ciclistas_kilometros_df = tarea1_funciones.obtener_kilometros_por_ciclista(ciclista_actividad_ruta_df)

    print("Kilómetros recorridos por ciclista, por ruta, por provincia y por día:")
    ciclistas_kilometros_df.show()

    #indica el top N de cilclistas por provincia que se quieren obtener
    N = args.N

    provincia_ciclistas_kilometros_total_df = tarea1_funciones.obtener_topN_ciclistas_por_provincia_en_total_de_kilometros (ciclistas_kilometros_df, N)
    print("Top", N, "de ciclistas por provincia, en total de kilómetros:")
    provincia_ciclistas_kilometros_total_df.show()

    provincia_ciclistas_kilometros_promedio_df = tarea1_funciones.obtener_topN_ciclistas_por_provincia_en_promedio_de_kilometros_por_dia (ciclistas_kilometros_df, N)
    print("Top", N, "de ciclistas por provincia, en promedio de kilómetros por día:")
    provincia_ciclistas_kilometros_promedio_df.show()
    
    top_N_ciclistas_por_provincia = tarea1_funciones.unir_dataframes_Top_N_ciclistas_por_provincia(provincia_ciclistas_kilometros_total_df, provincia_ciclistas_kilometros_promedio_df)
    print("Top", N, "de ciclistas por provincia, tanto en total de kilómetros como en promedio de kilómetros por día:")
    top_N_ciclistas_por_provincia.show()

 
programaPrincipal()