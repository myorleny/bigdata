from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType, FloatType, StructField,
                               StructType, TimestampType, StringType, DateType)
from pyspark.sql.functions import col, date_format, udf, rank, lit, percentile_approx                              
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Viajes").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

def obtener_total_viajes_por_codigo_postal_origen (viajes_didier_df):

    #excluye los registros con kilómetros o precio_kilometro en 0, negativo o en null
    viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.kilometros > 0)
    viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.precio_kilometro > 0)
    
    total_viajes_por_codigo_postal_origen_df = viajes_didier_df.groupBy("codigo_postal_origen").count()

    total_viajes_por_codigo_postal_origen_df = total_viajes_por_codigo_postal_origen_df.withColumn("Origen_Destino",lit("Origen"))

    total_viajes_por_codigo_postal_origen_df = total_viajes_por_codigo_postal_origen_df.select(
        col("codigo_postal_origen").alias("Codigo_Postal"),
        col("Origen_Destino"),
        col("count").alias("Cantidad_Total_Viajes"))

    total_viajes_por_codigo_postal_origen_df = total_viajes_por_codigo_postal_origen_df.orderBy(col("Codigo_Postal").asc())

    return total_viajes_por_codigo_postal_origen_df    

def obtener_total_viajes_por_codigo_postal_destino (viajes_didier_df):
    
    #excluye los registros con kilómetros o precio_kilometro en 0, negativo o en null
    viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.kilometros > 0)
    viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.precio_kilometro > 0)

    total_viajes_por_codigo_postal_destino_df = viajes_didier_df.groupBy("codigo_postal_destino").count()

    total_viajes_por_codigo_postal_destino_df = total_viajes_por_codigo_postal_destino_df.withColumn("Origen_Destino",lit("Destino"))

    total_viajes_por_codigo_postal_destino_df = total_viajes_por_codigo_postal_destino_df.select(
        col("codigo_postal_destino").alias("Codigo_Postal"),
        col("Origen_Destino"),
        col("count").alias("Cantidad_Total_Viajes"))

    total_viajes_por_codigo_postal_destino_df = total_viajes_por_codigo_postal_destino_df.orderBy(col("Codigo_Postal").asc())

    return total_viajes_por_codigo_postal_destino_df    

def unir_dataframes_total_viajes_por_codigo_postal_origen_destino(total_viajes_por_codigo_postal_origen_df, total_viajes_por_codigo_postal_destino_df):
    
    total_viajes_por_codigo_postal_df = total_viajes_por_codigo_postal_origen_df.union(total_viajes_por_codigo_postal_destino_df)

    return total_viajes_por_codigo_postal_df

def obtener_total_ingresos_por_codigo_postal_origen (viajes_didier_df):
    
    #excluye los registros con kilómetros o precio_kilometro en 0, negativo o en null
    viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.kilometros > 0)
    viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.precio_kilometro > 0)

    viajes_didier_df = viajes_didier_df.withColumn("Ingreso_por_Viaje",col("kilometros")*col("precio_kilometro"))
    #viajes_didier_df.show()

    total_ingresos_por_codigo_postal_origen_df = viajes_didier_df.groupBy("codigo_postal_origen").sum("Ingreso_por_Viaje")

    total_ingresos_por_codigo_postal_origen_df = total_ingresos_por_codigo_postal_origen_df.withColumn("Origen_Destino",lit("Origen"))

    total_ingresos_por_codigo_postal_origen_df = total_ingresos_por_codigo_postal_origen_df.select(
        col("codigo_postal_origen").alias("Codigo_Postal"),
        col("Origen_Destino"),
        col("sum(Ingreso_por_Viaje)").alias("Cantidad_total_ingresos"))

    total_ingresos_por_codigo_postal_origen_df = total_ingresos_por_codigo_postal_origen_df.orderBy(col("Codigo_Postal").asc())

    return total_ingresos_por_codigo_postal_origen_df    

def obtener_total_ingresos_por_codigo_postal_destino (viajes_didier_df):
    
    #excluye los registros con kilómetros o precio_kilometro en 0, negativos o en null
    viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.kilometros > 0)
    viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.precio_kilometro > 0)

    viajes_didier_df = viajes_didier_df.withColumn("Ingreso_por_Viaje",col("kilometros")*col("precio_kilometro"))
    viajes_didier_df.show()

    total_ingresos_por_codigo_postal_destino_df = viajes_didier_df.groupBy("codigo_postal_destino").sum("Ingreso_por_Viaje")

    total_ingresos_por_codigo_postal_destino_df = total_ingresos_por_codigo_postal_destino_df.withColumn("Origen_Destino",lit("Destino"))

    total_ingresos_por_codigo_postal_destino_df = total_ingresos_por_codigo_postal_destino_df.select(
        col("codigo_postal_destino").alias("Codigo_Postal"),
        col("Origen_Destino"),
        col("sum(Ingreso_por_Viaje)").alias("Cantidad_total_ingresos"))

    total_ingresos_por_codigo_postal_destino_df = total_ingresos_por_codigo_postal_destino_df.orderBy(col("Codigo_Postal").asc())

    return total_ingresos_por_codigo_postal_destino_df   

def unir_dataframes_total_ingresos_por_codigo_postal_origen_destino(total_ingresos_por_codigo_postal_origen_df, total_ingresos_por_codigo_postal_destino_df):
    
    total_ingresos_por_codigo_postal_df = total_ingresos_por_codigo_postal_origen_df.union(total_ingresos_por_codigo_postal_destino_df)

    return total_ingresos_por_codigo_postal_df    


def obtener_metrica_persona_con_mas_kilometros (viajes_didier_df):
    
    #excluye los registros con kilómetros o precio_kilometro en 0, negativos o en null
    viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.kilometros > 0)
    viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.precio_kilometro > 0)

    viajes_didier_df = viajes_didier_df.select(col("identificador"),viajes_didier_df.kilometros.cast(FloatType()))
    persona_con_mas_kilometros_df = viajes_didier_df.groupBy("identificador").sum("kilometros")

    persona_con_mas_kilometros_df = persona_con_mas_kilometros_df.orderBy(col("sum(kilometros)").desc(),col("identificador").asc()).limit(1)
    persona_con_mas_kilometros_df = persona_con_mas_kilometros_df.withColumn("Tipo_de_Metrica",lit("persona_con_mas_kilometros"))
    persona_con_mas_kilometros_df = persona_con_mas_kilometros_df.select(col("Tipo_de_Metrica"),col("identificador").alias("Valor"))
    
    return persona_con_mas_kilometros_df

def obtener_metrica_persona_con_mas_ingresos (viajes_didier_df):
    
    #excluye los registros con kilómetros o precio_kilometro en 0, negativos o en null
    viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.kilometros > 0)
    viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.precio_kilometro > 0)

    viajes_didier_df = viajes_didier_df.withColumn("Ingreso_por_Viaje",col("kilometros")*col("precio_kilometro"))
    #viajes_didier_df = viajes_didier_df.select(col("identificador"),viajes_didier_df.kilometros.cast(FloatType()))
    persona_con_mas_ingresos_df = viajes_didier_df.groupBy("identificador").sum("Ingreso_por_Viaje")

    persona_con_mas_ingresos_df = persona_con_mas_ingresos_df.orderBy(col("sum(Ingreso_por_Viaje)").desc(),col("identificador").asc()).limit(1)
    persona_con_mas_ingresos_df = persona_con_mas_ingresos_df.withColumn("Tipo_de_Metrica",lit("persona_con_mas_ingresos"))
    persona_con_mas_ingresos_df = persona_con_mas_ingresos_df.select(col("Tipo_de_Metrica"),col("identificador").alias("Valor"))
    
    return persona_con_mas_ingresos_df
    
def calcular_metrica_percentil (viajes_didier_df, percentil):
    
    #excluye los registros con kilómetros o precio_kilometro en 0, negativos o en null
    viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.kilometros > 0)
    viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.precio_kilometro > 0)

    viajes_didier_df = viajes_didier_df.withColumn("Ingreso_por_Viaje",col("kilometros")*col("precio_kilometro"))
    personas_ingresos_df = viajes_didier_df.groupBy("identificador").sum("Ingreso_por_Viaje")

    personas_ingresos_df = personas_ingresos_df.orderBy(col("sum(Ingreso_por_Viaje)").asc(),col("identificador").asc())
    #personas_ingresos_df.show()


    # valor2_percentil_df = personas_ingresos_df.approxQuantile("sum(Ingreso_por_Viaje)",[percentil/100],0)
    # print (valor2_percentil_df)
    
    metrica = "percentil_" + str(percentil) 
    valor_percentil_df = personas_ingresos_df.select(percentile_approx("sum(Ingreso_por_Viaje)", [percentil/100]).alias("Valor"))
    valor_percentil_df = valor_percentil_df.withColumn("Tipo_de_Metrica",lit(metrica))
    valor_percentil_df = valor_percentil_df.select (col("Tipo_de_Metrica"), valor_percentil_df.Valor.cast(StringType()))
    
    return valor_percentil_df    

def obtener_metrica_codigo_postal_origen_con_mas_ingresos (viajes_didier_df):
    
    #excluye los registros con kilómetros o precio_kilometro en 0, negativos o en null
    viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.kilometros > 0)
    viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.precio_kilometro > 0)

    viajes_didier_df = viajes_didier_df.withColumn("Ingreso_por_Viaje",col("kilometros")*col("precio_kilometro"))
    #viajes_didier_df.show()

    total_ingresos_por_codigo_postal_origen_df = viajes_didier_df.groupBy("codigo_postal_origen").sum("Ingreso_por_Viaje")

    codigo_postal_origen_con_mas_ingresos_df = total_ingresos_por_codigo_postal_origen_df.orderBy(col("sum(Ingreso_por_Viaje)").desc(),col("codigo_postal_origen").asc()).limit(1)

    codigo_postal_origen_con_mas_ingresos_df = codigo_postal_origen_con_mas_ingresos_df.withColumn("Tipo_de_Metrica",lit("codigo_postal_origen_con_mas_ingresos"))
    codigo_postal_origen_con_mas_ingresos_df = codigo_postal_origen_con_mas_ingresos_df.select(col("Tipo_de_Metrica"),col("codigo_postal_origen").alias("Valor"))

    return codigo_postal_origen_con_mas_ingresos_df

def obtener_metrica_codigo_postal_destino_con_mas_ingresos (viajes_didier_df):

    #excluye los registros con kilómetros o precio_kilometro en 0, negativos o en null
    viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.kilometros > 0)
    viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.precio_kilometro > 0)

    viajes_didier_df = viajes_didier_df.withColumn("Ingreso_por_Viaje",col("kilometros")*col("precio_kilometro"))
    #viajes_didier_df.show()

    total_ingresos_por_codigo_postal_destino_df = viajes_didier_df.groupBy("codigo_postal_destino").sum("Ingreso_por_Viaje")

    codigo_postal_destino_con_mas_ingresos_df = total_ingresos_por_codigo_postal_destino_df.orderBy(col("sum(Ingreso_por_Viaje)").desc(),col("codigo_postal_destino").asc()).limit(1)

    codigo_postal_destino_con_mas_ingresos_df = codigo_postal_destino_con_mas_ingresos_df.withColumn("Tipo_de_Metrica",lit("codigo_postal_destino_con_mas_ingresos"))
    codigo_postal_destino_con_mas_ingresos_df = codigo_postal_destino_con_mas_ingresos_df.select(col("Tipo_de_Metrica"),col("codigo_postal_destino").alias("Valor"))

    return codigo_postal_destino_con_mas_ingresos_df    

def unir_dataframes_metricas(metrica_persona_con_mas_kilometros_df, metrica_persona_con_mas_ingresos_df, metrica_valor_percentil_25_df, metrica_valor_percentil_50_df, metrica_valor_percentil_75_df, metrica_codigo_postal_origen_con_mas_ingresos_df, metrica_codigo_postal_destino_con_mas_ingresos_df):
    
    metricas_df = metrica_persona_con_mas_kilometros_df.union(metrica_persona_con_mas_ingresos_df)
    metricas_df = metricas_df.union(metrica_valor_percentil_25_df)
    metricas_df = metricas_df.union(metrica_valor_percentil_50_df)
    metricas_df = metricas_df.union(metrica_valor_percentil_75_df)
    metricas_df = metricas_df.union(metrica_codigo_postal_origen_con_mas_ingresos_df)
    metricas_df = metricas_df.union(metrica_codigo_postal_destino_con_mas_ingresos_df)

    return metricas_df       