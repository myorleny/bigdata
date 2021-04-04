from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType, FloatType, StructField,
                               StructType, TimestampType, StringType, DateType)
from pyspark.sql.functions import col, date_format, udf, rank, lit                              
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Actividades de Ciclistas").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

# Une los datos de los tres archivos
def join_dataframes(ciclista_df, ruta_df, actividad_df):
    
    ciclista_actividad_df = ciclista_df.join(actividad_df, ciclista_df.cedula == actividad_df.cedula_Ciclista)

    ciclista_actividad_ruta_df = ciclista_actividad_df.join(ruta_df, ciclista_actividad_df.codigo_Ruta == ruta_df.codigo)
  
    return ciclista_actividad_ruta_df

# Obtiene kilómetros recorridos por ciclista, por ruta, por provincia y por día
def obtener_kilometros_por_ciclista (ciclista_actividad_ruta_df):
    
    #excluye los registros con kilómetros en 0, negativos o en null
    filter_df = ciclista_actividad_ruta_df.filter(ciclista_actividad_ruta_df.kilometros > 0)
    
    sum_df = filter_df.groupBy("cedula", "nombre_Completo", "codigo", "nombre_Ruta", "provincia", "fecha").sum("kilometros")
    #sum_df.show()

    ciclistas_kilometros_df = \
        sum_df.select(
            col('cedula'),
            col('nombre_Completo'),
            col('codigo'),
            col('nombre_Ruta'),
            col('provincia'),
            col('fecha'),
            col('sum(kilometros)').alias('TotalKilometros'))

    return ciclistas_kilometros_df

# Obtiene el top N de ciclistas por provincia, en total de kilómetros 
def obtener_topN_ciclistas_por_provincia_en_total_de_kilometros (ciclistas_kilometros_df, N):
    
    #obtiene el total de kilómetros por ciclista, agrupado por provincia, cedula y nombre_Completo
    provincia_ciclistas_kilometros_total_df = ciclistas_kilometros_df.groupBy("provincia", "cedula", "nombre_Completo").sum("TotalKilometros")
    #provincia_ciclistas_kilometros_total_df.show()

    provincia_ciclistas_kilometros_total_df = \
    provincia_ciclistas_kilometros_total_df.select(
        col('provincia'),
        col('cedula'),
        col('nombre_Completo'),
        col('sum(TotalKilometros)').alias('TotalKilometros'))
    #provincia_ciclistas_kilometros_total_df.show()

    #particiona los datos por provincia, ordenados por TotalKilometros descendente y cedula ascendente, y posteriormente crea columna para asignarles una "posición"
    window = Window.partitionBy('provincia').orderBy(col('TotalKilometros').desc(),col('cedula').asc())
    provincia_ciclistas_kilometros_total_df = provincia_ciclistas_kilometros_total_df.withColumn("Posicion_Por_Provincia",rank().over(window))

    provincia_ciclistas_kilometros_total_df = provincia_ciclistas_kilometros_total_df.withColumn("Tipo_Top_N_Ciclistas_Por_Provincia",lit("Total de Km"))
	
    #obtiene el top N
    provincia_ciclistas_kilometros_total_df = provincia_ciclistas_kilometros_total_df.filter(provincia_ciclistas_kilometros_total_df.Posicion_Por_Provincia <= N)

    provincia_ciclistas_kilometros_total_df = provincia_ciclistas_kilometros_total_df.select(
    col('Tipo_Top_N_Ciclistas_Por_Provincia'),
    col('provincia'),
    col('cedula'),
    col('nombre_Completo'),
    col('TotalKilometros').alias('Valor'),
    col('Posicion_Por_Provincia'))  
	
    return provincia_ciclistas_kilometros_total_df

# Obtiene el top N de ciclistas por provincia, en promedio de kilómetros por día 
def obtener_topN_ciclistas_por_provincia_en_promedio_de_kilometros_por_dia (ciclistas_kilometros_df, N):
    
    #Obtiene el total de kilómetros por persona por día para luego poder obtener la cantidad de días que tuvo actividad
    total_km_por_ciclista_por_dia_df = ciclistas_kilometros_df.groupBy("cedula", "fecha").sum("TotalKilometros")
    #Obtiene la cantidad de días en que cada ciclista tuvo actividad
    cantidad_dias_ciclista_df = total_km_por_ciclista_por_dia_df.groupBy("cedula").count()
    cantidad_dias_ciclista_df = cantidad_dias_ciclista_df.select(
        col('cedula').alias('cedula_ciclista'),
        col('count').alias('CantidadDias'))
    #Obtiene el total de kilómetros por ciclista
    total_km_por_ciclista_df = ciclistas_kilometros_df.groupBy("provincia", "cedula", "nombre_Completo").sum("TotalKilometros")
    #hace join del dataframe que contiene el total de kilómetros por ciclista con el que contiene la cantidad de día que cada ciclista tuvo actividad
    provincia_ciclistas_kilometros_promedio_df = total_km_por_ciclista_df.join(cantidad_dias_ciclista_df, total_km_por_ciclista_df.cedula == cantidad_dias_ciclista_df.cedula_ciclista)
    provincia_ciclistas_kilometros_promedio_df = provincia_ciclistas_kilometros_promedio_df.select(
        col('provincia'),
        col('cedula'),
        col('nombre_Completo'),
        col('sum(TotalKilometros)').alias('TotalKilometros'),
        col('CantidadDias'))
    #Agrega nueva columna que calcula el promedio de kilómetros por ciclista
    provincia_ciclistas_kilometros_promedio_df = provincia_ciclistas_kilometros_promedio_df.withColumn("Promedio_Km_Por_Dia",provincia_ciclistas_kilometros_promedio_df.TotalKilometros/provincia_ciclistas_kilometros_promedio_df.CantidadDias)
    #provincia_ciclistas_kilometros_promedio_df.show()

    #particiona los datos por provincia, ordenados por Promedio_Km_Por_Dia descendente y cedula ascendente, y posteriormente crea columna para asignarles una "posición"
    window = Window.partitionBy('provincia').orderBy(col('Promedio_Km_Por_Dia').desc(),col('cedula').asc())
    provincia_ciclistas_kilometros_promedio_df = provincia_ciclistas_kilometros_promedio_df.withColumn("Posicion_Por_Provincia",rank().over(window))

    provincia_ciclistas_kilometros_promedio_df = provincia_ciclistas_kilometros_promedio_df.withColumn("Tipo_Top_N_Ciclistas_Por_Provincia",lit("Promedio de Km/día"))
	
    #obtiene el top N
    provincia_ciclistas_kilometros_promedio_df = provincia_ciclistas_kilometros_promedio_df.filter(provincia_ciclistas_kilometros_promedio_df.Posicion_Por_Provincia <= N)
    #provincia_ciclistas_kilometros_promedio_df.show()

    provincia_ciclistas_kilometros_promedio_df = provincia_ciclistas_kilometros_promedio_df.select(
    col('Tipo_Top_N_Ciclistas_Por_Provincia'),
    col('provincia'),
    col('cedula'),
    col('nombre_Completo'),
    col('Promedio_Km_Por_Dia').alias('Valor'),
    col('Posicion_Por_Provincia'))  
	
    return provincia_ciclistas_kilometros_promedio_df    

# Une los dataframes que contienen el top N de ciclistas por provincia, en total de kilómetros y en promedio de kilómetros por día
def unir_dataframes_Top_N_ciclistas_por_provincia(provincia_ciclistas_kilometros_total_df, provincia_ciclistas_kilometros_promedio_df):
    
    top_N_ciclistas_por_provincia = provincia_ciclistas_kilometros_total_df.union(provincia_ciclistas_kilometros_promedio_df)

    return top_N_ciclistas_por_provincia