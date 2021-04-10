from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType, FloatType, StructField,
                               StructType, TimestampType, StringType, DateType)
from pyspark.sql.functions import col, date_format, udf, rank, lit                              
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Viajes").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

def obtener_total_viajes_por_codigo_postal_origen (viajes_didier_df):
    
    total_viajes_por_codigo_postal_origen_df = viajes_didier_df.groupBy("codigo_postal_origen").count()

    total_viajes_por_codigo_postal_origen_df = total_viajes_por_codigo_postal_origen_df.withColumn("Origen_Destino",lit("Origen"))

    total_viajes_por_codigo_postal_origen_df = total_viajes_por_codigo_postal_origen_df.select(
        col('codigo_postal_origen').alias('Codigo_Postal'),
        col('Origen_Destino'),
        col('count').alias('Cantidad_Total_Viajes'))

    total_viajes_por_codigo_postal_origen_df = total_viajes_por_codigo_postal_origen_df.orderBy(col('Codigo_Postal').asc())

    return total_viajes_por_codigo_postal_origen_df    

def obtener_total_viajes_por_codigo_postal_destino (viajes_didier_df):
    
    total_viajes_por_codigo_postal_destino_df = viajes_didier_df.groupBy("codigo_postal_destino").count()

    total_viajes_por_codigo_postal_destino_df = total_viajes_por_codigo_postal_destino_df.withColumn("Origen_Destino",lit("Destino"))

    total_viajes_por_codigo_postal_destino_df = total_viajes_por_codigo_postal_destino_df.select(
        col('codigo_postal_destino').alias('Codigo_Postal'),
        col('Origen_Destino'),
        col('count').alias('Cantidad_Total_Viajes'))

    total_viajes_por_codigo_postal_destino_df = total_viajes_por_codigo_postal_destino_df.orderBy(col('Codigo_Postal').asc())

    return total_viajes_por_codigo_postal_destino_df    

def unir_dataframes_total_viajes_por_codigo_postal_origen_destino(total_viajes_por_codigo_postal_origen_df, total_viajes_por_codigo_postal_destino_df):
    
    total_viajes_por_codigo_postal_df = total_viajes_por_codigo_postal_origen_df.union(total_viajes_por_codigo_postal_destino_df)

    return total_viajes_por_codigo_postal_df

def obtener_total_ingresos_por_codigo_postal_origen (viajes_didier_df):
    
    viajes_didier_df = viajes_didier_df.withColumn("Ingreso_por_Viaje",col('kilometros')*col('precio_kilometro'))
    viajes_didier_df.show()

    total_ingresos_por_codigo_postal_origen_df = viajes_didier_df.groupBy("codigo_postal_origen").sum("Ingreso_por_Viaje")

    total_ingresos_por_codigo_postal_origen_df = total_ingresos_por_codigo_postal_origen_df.withColumn("Origen_Destino",lit("Origen"))

    total_ingresos_por_codigo_postal_origen_df = total_ingresos_por_codigo_postal_origen_df.select(
        col('codigo_postal_origen').alias('Codigo_Postal'),
        col('Origen_Destino'),
        col('sum(Ingreso_por_Viaje)').alias('Cantidad_total_ingresos'))

    total_ingresos_por_codigo_postal_origen_df = total_ingresos_por_codigo_postal_origen_df.orderBy(col('Codigo_Postal').asc())

    return total_ingresos_por_codigo_postal_origen_df    

def obtener_total_ingresos_por_codigo_postal_destino (viajes_didier_df):
    
    #agregar filtro para incluir solo kilometros > 0 y precio_kilometro > 0
    viajes_didier_df = viajes_didier_df.withColumn("Ingreso_por_Viaje",col('kilometros')*col('precio_kilometro'))
    viajes_didier_df.show()

    total_ingresos_por_codigo_postal_destino_df = viajes_didier_df.groupBy("codigo_postal_destino").sum("Ingreso_por_Viaje")

    total_ingresos_por_codigo_postal_destino_df = total_ingresos_por_codigo_postal_destino_df.withColumn("Origen_Destino",lit("Destino"))

    total_ingresos_por_codigo_postal_destino_df = total_ingresos_por_codigo_postal_destino_df.select(
        col('codigo_postal_destino').alias('Codigo_Postal'),
        col('Origen_Destino'),
        col('sum(Ingreso_por_Viaje)').alias('Cantidad_total_ingresos'))

    total_ingresos_por_codigo_postal_destino_df = total_ingresos_por_codigo_postal_destino_df.orderBy(col('Codigo_Postal').asc())

    return total_ingresos_por_codigo_postal_destino_df   

def unir_dataframes_total_ingresos_por_codigo_postal_origen_destino(total_ingresos_por_codigo_postal_origen_df, total_ingresos_por_codigo_postal_destino_df):
    
    total_ingresos_por_codigo_postal_df = total_ingresos_por_codigo_postal_origen_df.union(total_ingresos_por_codigo_postal_destino_df)

    return total_ingresos_por_codigo_postal_df    


def obtener_persona_con_mas_kilometros (viajes_didier_df):
    
    #excluye los registros con kilómetros en 0, negativos o en null
    #filter_df = ciclista_actividad_ruta_df.filter(ciclista_actividad_ruta_df.kilometros > 0)
    viajes_didier_df = viajes_didier_df.select(col("identificador"),viajes_didier_df.kilometros.cast(FloatType()))
    persona_con_mas_kilometros_df = viajes_didier_df.groupBy("identificador").sum("kilometros")

    persona_con_mas_kilometros_df = persona_con_mas_kilometros_df.orderBy(col('sum(kilometros)').desc(),col('identificador').asc()).limit(1)
    persona_con_mas_kilometros_df = persona_con_mas_kilometros_df.withColumn("Tipo_de_Metrica",lit("persona_con_mas_kilometros"))
    persona_con_mas_kilometros_df = persona_con_mas_kilometros_df.select(col("Tipo_de_Metrica"),col("identificador").alias('Valor'))

    return persona_con_mas_kilometros_df

def obtener_persona_con_mas_ingresos (viajes_didier_df):
    
    #excluye los registros con kilómetros en 0, negativos o en null
    #filter_df = ciclista_actividad_ruta_df.filter(ciclista_actividad_ruta_df.kilometros > 0)
    viajes_didier_df = viajes_didier_df.withColumn("Ingreso_por_Viaje",col('kilometros')*col('precio_kilometro'))
    #viajes_didier_df = viajes_didier_df.select(col("identificador"),viajes_didier_df.kilometros.cast(FloatType()))
    persona_con_mas_ingresos_df = viajes_didier_df.groupBy("identificador").sum("Ingreso_por_Viaje")

    persona_con_mas_ingresos_df = persona_con_mas_ingresos_df.orderBy(col('sum(Ingreso_por_Viaje)').desc(),col('identificador').asc()).limit(1)
    persona_con_mas_ingresos_df = persona_con_mas_ingresos_df.withColumn("Tipo_de_Metrica",lit("persona_con_mas_ingresos"))
    persona_con_mas_ingresos_df = persona_con_mas_ingresos_df.select(col("Tipo_de_Metrica"),col("identificador").alias('Valor'))

    return persona_con_mas_ingresos_df
    