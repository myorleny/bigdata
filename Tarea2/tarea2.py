from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType, FloatType, StructField,
                               StructType, TimestampType, StringType, DateType)
from pyspark.sql.functions import col, date_format, udf, rank, lit      
from pyspark.sql.functions import explode                               

import tarea2_funciones

import sys

spark = SparkSession.builder.appName("Viajes").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

def CargarArchivosJSON(args):
    
    #Crea un dataframe 
    viajes_schema = StructType([StructField('identificador', IntegerType()),
                            StructField('codigo_postal_origen', IntegerType()),
                            StructField('codigo_postal_destino', IntegerType()),
                            StructField('kilometros', FloatType()),
                            StructField('precio_kilometro', FloatType()),
                            ])

    viajes_didier_df = spark.createDataFrame([], viajes_schema)
    
    #Carga los viajes de cada persona en el dataframe recién creado
    archivosCargados = 0
    for arg in args:
        if arg[-5:] == ".json":
            print(arg)
            viajes_por_persona_df = spark.read.option("multiline","true").json(arg)
            viajes_por_persona_df = viajes_por_persona_df.withColumn("viajes", explode(viajes_por_persona_df.viajes))
            viajes_didier_df = viajes_didier_df.union(viajes_por_persona_df.select("identificador", "viajes.codigo_postal_origen", "viajes.codigo_postal_destino", "viajes.kilometros","viajes.precio_kilometro"))
            archivosCargados = archivosCargados + 1

    if archivosCargados == 0:
        print("No se encontro ningun archivo .JSON")
        exit()

    return viajes_didier_df


def main(args):
    #llama a la función que carga los archivos .json en un dataframe
    viajes_didier_df = CargarArchivosJSON(args)
    
    #llama a las funciones para obtener el total de viajes por código postal tanto origen como destino
    total_viajes_por_codigo_postal_origen_df = tarea2_funciones.obtener_total_viajes_por_codigo_postal_origen (viajes_didier_df)
    total_viajes_por_codigo_postal_destino_df = tarea2_funciones.obtener_total_viajes_por_codigo_postal_destino (viajes_didier_df)
    total_viajes_por_codigo_postal_df = tarea2_funciones.unir_dataframes(total_viajes_por_codigo_postal_origen_df, total_viajes_por_codigo_postal_destino_df) 
    total_viajes_por_codigo_postal_df.show()
    #almacena en un archivo csv el dataframe que contiene el total de viajes por código postal tanto origen como destino
    total_viajes_por_codigo_postal_df.coalesce(1).write.format('csv').option('header',True).mode('overwrite').option('sep',',').save("total_viajes")

    #llama a las funciones para obtener el total de ingresos por código postal tanto origen como destino
    total_ingresos_por_codigo_postal_origen_df = tarea2_funciones.obtener_total_ingresos_por_codigo_postal_origen (viajes_didier_df)
    total_ingresos_por_codigo_postal_destino_df = tarea2_funciones.obtener_total_ingresos_por_codigo_postal_destino (viajes_didier_df)
    total_ingresos_por_codigo_postal_df = tarea2_funciones.unir_dataframes(total_ingresos_por_codigo_postal_origen_df, total_ingresos_por_codigo_postal_destino_df)   
    total_ingresos_por_codigo_postal_df.show()
    #almacena en un archivo csv el dataframe que contiene el total de ingresos por código postal tanto origen como destino
    total_ingresos_por_codigo_postal_df.coalesce(1).write.format('csv').option('header',True).mode('overwrite').option('sep',',').save("total_ingresos")

    #llama a las funciones que calculan cada una de las métricas
    metrica_persona_con_mas_kilometros_df = tarea2_funciones.obtener_metrica_persona_con_mas_kilometros(viajes_didier_df)
    metrica_persona_con_mas_ingresos_df = tarea2_funciones.obtener_metrica_persona_con_mas_ingresos(viajes_didier_df)
    metrica_valor_percentil_25_df = tarea2_funciones.calcular_metrica_percentil (viajes_didier_df, 25)
    metrica_valor_percentil_50_df = tarea2_funciones.calcular_metrica_percentil (viajes_didier_df, 50)
    metrica_valor_percentil_75_df = tarea2_funciones.calcular_metrica_percentil (viajes_didier_df, 75)
    metrica_codigo_postal_origen_con_mas_ingresos_df = tarea2_funciones.obtener_metrica_codigo_postal_origen_con_mas_ingresos (viajes_didier_df)
    metrica_codigo_postal_destino_con_mas_ingresos_df = tarea2_funciones.obtener_metrica_codigo_postal_destino_con_mas_ingresos (viajes_didier_df)
    #guarda todas las métricas en un solo dataframe
    metricas_df = tarea2_funciones.unir_dataframes_metricas(metrica_persona_con_mas_kilometros_df, metrica_persona_con_mas_ingresos_df, metrica_valor_percentil_25_df, metrica_valor_percentil_50_df, metrica_valor_percentil_75_df, metrica_codigo_postal_origen_con_mas_ingresos_df, metrica_codigo_postal_destino_con_mas_ingresos_df)
    metricas_df.show()  
    #almacena en un archivo csv el dataframe que contiene cada una de las métricas con su respectivo valor
    metricas_df.coalesce(1).write.format('csv').option('header',True).mode('overwrite').option('sep',',').save("metricas")  

 
if __name__ == '__main__':
    import sys
    sys.exit(main(sys.argv))