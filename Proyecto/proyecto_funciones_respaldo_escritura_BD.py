
# findspark.add_packages('mysql:mysql-connector-java:8.0.11')

from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType, FloatType, StructField,
                               StructType, TimestampType, StringType, DateType)
from pyspark.sql.functions import col, date_format, udf, rank, lit, explode, percentile_approx                              
from pyspark.sql.window import Window
from pyspark import SparkContext, SparkConf
import os
import sys
import subprocess as sp

spark = SparkSession \
    .builder \
    .appName("Basic JDBC pipeline") \
    .config("spark.driver.extraClassPath", 'postgresql-42.2.14.jar') \
    .config("spark.executor.extraClassPath", 'postgresql-42.2.14.jar') \
    .config("spark.jars", 'postgresql-42.2.14.jar') \
    .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

def cargar_archivos_csv():
    escuelas_df = spark \
        .read \
        .format("csv") \
        .option("path", "./Fuentes_de_datos/megabaseprimaria_2015.csv") \
        .option("header", True) \
        .option("inferschema", "true") \
        .option("delimiter", ",") \
        .load()

    ids_df = spark \
        .read \
        .format("csv") \
        .option("path", "./Fuentes_de_datos/IDS_distrital_cantonal.csv") \
        .option("header", True) \
        .option("inferschema", "true") \
        .option("delimiter", ",") \
        .load()   

    escuelas_ids_df = escuelas_df.join(ids_df, escuelas_df.cddis15 == ids_df.Codigo)             

    print (sys.path)
    
    escuelas_df \
        .write \
        .mode("overwrite") \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://host.docker.internal:5433/postgres") \
        .option("user", "postgres") \
        .option("password", "testPassword") \
        .option("dbtable", "escuelas") \
        .option("driver", "org.postgresql.Driver") \
        .save()        

    # escuelas_df.show()  
    # ids_df.show()
    # escuelas_ids_df.show()
    # escuelas_df.printSchema()
    # ids_df.printSchema()
    # print (escuelas_df.count())
    # print (ids_df.count())
    # print (escuelas_ids_df.count())



    

cargar_archivos_csv()         
        


    # #Crea un dataframe 
    # viajes_schema = StructType([StructField('identificador', IntegerType()),
    #                         StructField('codigo_postal_origen', IntegerType()),
    #                         StructField('codigo_postal_destino', IntegerType()),
    #                         StructField('kilometros', FloatType()),
    #                         StructField('precio_kilometro', FloatType()),
    #                         ])

    # viajes_didier_df = spark.createDataFrame([], viajes_schema)
    
    # #Carga los viajes de cada persona en el dataframe reci??n creado
    # archivosCargados = 0
    # for arg in args:
    #     if arg[-5:] == ".json":
    #         viajes_por_persona_df = spark.read.option("multiline","true").json(arg)
    #         viajes_por_persona_df = viajes_por_persona_df.withColumn("viajes", explode(viajes_por_persona_df.viajes))
    #         viajes_didier_df = viajes_didier_df.union(viajes_por_persona_df.select("identificador", "viajes.codigo_postal_origen", "viajes.codigo_postal_destino", "viajes.kilometros","viajes.precio_kilometro"))
    #         archivosCargados = archivosCargados + 1

    # if archivosCargados == 0:
    #     print("No se encontro ning??n archivo .json")
    #     exit()

    # return viajes_didier_df

# def obtener_total_viajes_por_codigo_postal_origen (viajes_didier_df):

#     #excluye los registros con kil??metros o precio_kilometro en 0, negativo o en null
#     viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.kilometros > 0)
#     viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.precio_kilometro > 0)
#     #excluye c??digos postales origen inv??lidos (0, negativo, null)
#     viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.codigo_postal_origen > 0)
    
#     #obtiene la cantidad de viajes por codigo_postal_origen
#     total_viajes_por_codigo_postal_origen_df = viajes_didier_df.groupBy("codigo_postal_origen").count()

#     total_viajes_por_codigo_postal_origen_df = total_viajes_por_codigo_postal_origen_df.withColumn("Origen_Destino",lit("Origen"))

#     total_viajes_por_codigo_postal_origen_df = total_viajes_por_codigo_postal_origen_df.select(
#         col("codigo_postal_origen").alias("Codigo_Postal"),
#         col("Origen_Destino"),
#         col("count").alias("Cantidad_Total_Viajes"))

#     total_viajes_por_codigo_postal_origen_df = total_viajes_por_codigo_postal_origen_df.orderBy(col("Codigo_Postal").asc())

#     return total_viajes_por_codigo_postal_origen_df    

# def obtener_total_viajes_por_codigo_postal_destino (viajes_didier_df):
    
#     #excluye los registros con kil??metros o precio_kilometro en 0, negativo o en null
#     viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.kilometros > 0)
#     viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.precio_kilometro > 0)
#     #excluye c??digos postales destino inv??lidos (por ejemplo en null)
#     viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.codigo_postal_destino > 0)

#     #obtiene la cantidad de viajes por codigo_postal_destino
#     total_viajes_por_codigo_postal_destino_df = viajes_didier_df.groupBy("codigo_postal_destino").count()

#     total_viajes_por_codigo_postal_destino_df = total_viajes_por_codigo_postal_destino_df.withColumn("Origen_Destino",lit("Destino"))

#     total_viajes_por_codigo_postal_destino_df = total_viajes_por_codigo_postal_destino_df.select(
#         col("codigo_postal_destino").alias("Codigo_Postal"),
#         col("Origen_Destino"),
#         col("count").alias("Cantidad_Total_Viajes"))

#     total_viajes_por_codigo_postal_destino_df = total_viajes_por_codigo_postal_destino_df.orderBy(col("Codigo_Postal").asc())

#     return total_viajes_por_codigo_postal_destino_df    

# #funci??n para unir 2 datagrames
# def unir_dataframes(primer_df, segundo_df):
    
#     total_df = primer_df.union(segundo_df)

#     return total_df

# def obtener_total_ingresos_por_codigo_postal_origen (viajes_didier_df):
    
#     #excluye los registros con kil??metros o precio_kilometro en 0, negativo o en null
#     viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.kilometros > 0)
#     viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.precio_kilometro > 0)
#     #excluye c??digos postales origen inv??lidos  (0, negativo, null)
#     viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.codigo_postal_origen > 0)    

#     #obtiene el ingreso por viaje para posteriormente obtener el total de ingresos por cada codigo_postal_origen
#     viajes_didier_df = viajes_didier_df.withColumn("Ingreso_por_Viaje",col("kilometros")*col("precio_kilometro"))

#     total_ingresos_por_codigo_postal_origen_df = viajes_didier_df.groupBy("codigo_postal_origen").sum("Ingreso_por_Viaje")

#     total_ingresos_por_codigo_postal_origen_df = total_ingresos_por_codigo_postal_origen_df.withColumn("Origen_Destino",lit("Origen"))

#     total_ingresos_por_codigo_postal_origen_df = total_ingresos_por_codigo_postal_origen_df.select(
#         col("codigo_postal_origen").alias("Codigo_Postal"),
#         col("Origen_Destino"),
#         col("sum(Ingreso_por_Viaje)").alias("Cantidad_Total_Ingresos"))

#     total_ingresos_por_codigo_postal_origen_df = total_ingresos_por_codigo_postal_origen_df.orderBy(col("Codigo_Postal").asc())

#     return total_ingresos_por_codigo_postal_origen_df    

# def obtener_total_ingresos_por_codigo_postal_destino (viajes_didier_df):
    
#     #excluye los registros con kil??metros o precio_kilometro en 0, negativos o en null
#     viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.kilometros > 0)
#     viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.precio_kilometro > 0)
#     #excluye c??digos postales destino inv??lidos (por ejemplo en null)
#     viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.codigo_postal_destino > 0)

#     #obtiene el ingreso por viaje para posteriormente obtener el total de ingresos por cada codigo_postal_destino
#     viajes_didier_df = viajes_didier_df.withColumn("Ingreso_por_Viaje",col("kilometros")*col("precio_kilometro"))

#     total_ingresos_por_codigo_postal_destino_df = viajes_didier_df.groupBy("codigo_postal_destino").sum("Ingreso_por_Viaje")

#     total_ingresos_por_codigo_postal_destino_df = total_ingresos_por_codigo_postal_destino_df.withColumn("Origen_Destino",lit("Destino"))

#     total_ingresos_por_codigo_postal_destino_df = total_ingresos_por_codigo_postal_destino_df.select(
#         col("codigo_postal_destino").alias("Codigo_Postal"),
#         col("Origen_Destino"),
#         col("sum(Ingreso_por_Viaje)").alias("Cantidad_Total_Ingresos"))

#     total_ingresos_por_codigo_postal_destino_df = total_ingresos_por_codigo_postal_destino_df.orderBy(col("Codigo_Postal").asc())

#     return total_ingresos_por_codigo_postal_destino_df   


# def obtener_metrica_persona_con_mas_kilometros (viajes_didier_df):
    
#     #excluye los registros con kil??metros o precio_kilometro en 0, negativos o en null
#     viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.kilometros > 0)
#     viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.precio_kilometro > 0)

#     viajes_didier_df = viajes_didier_df.select(col("identificador"),viajes_didier_df.kilometros.cast(FloatType()))
#     #obtiene el total de Km por persona
#     persona_con_mas_kilometros_df = viajes_didier_df.groupBy("identificador").sum("kilometros")

#     #obtiene la persona con m??s Km
#     persona_con_mas_kilometros_df = persona_con_mas_kilometros_df.orderBy(col("sum(kilometros)").desc(),col("identificador").asc()).limit(1)

#     persona_con_mas_kilometros_df = persona_con_mas_kilometros_df.withColumn("Tipo_de_Metrica",lit("persona_con_mas_kilometros"))
#     persona_con_mas_kilometros_df = persona_con_mas_kilometros_df.select(col("Tipo_de_Metrica"),col("identificador").alias("Valor"))
    
#     return persona_con_mas_kilometros_df

# def obtener_metrica_persona_con_mas_ingresos (viajes_didier_df):
    
#     #excluye los registros con kil??metros o precio_kilometro en 0, negativos o en null
#     viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.kilometros > 0)
#     viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.precio_kilometro > 0)

#     viajes_didier_df = viajes_didier_df.withColumn("Ingreso_por_Viaje",col("kilometros")*col("precio_kilometro"))
    
#     #obtiene el total de ingresos por persona
#     persona_con_mas_ingresos_df = viajes_didier_df.groupBy("identificador").sum("Ingreso_por_Viaje")

#     #obtiene la persona con m??s ingresos
#     persona_con_mas_ingresos_df = persona_con_mas_ingresos_df.orderBy(col("sum(Ingreso_por_Viaje)").desc(),col("identificador").asc()).limit(1)

#     persona_con_mas_ingresos_df = persona_con_mas_ingresos_df.withColumn("Tipo_de_Metrica",lit("persona_con_mas_ingresos"))
#     persona_con_mas_ingresos_df = persona_con_mas_ingresos_df.select(col("Tipo_de_Metrica"),col("identificador").alias("Valor"))
    
#     return persona_con_mas_ingresos_df
    
# def calcular_metrica_percentil (viajes_didier_df, percentil):
    
#     #excluye los registros con kil??metros o precio_kilometro en 0, negativos o en null
#     viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.kilometros > 0)
#     viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.precio_kilometro > 0)

#     viajes_didier_df = viajes_didier_df.withColumn("Ingreso_por_Viaje",col("kilometros")*col("precio_kilometro"))

#     #obtiene el total de ingresos por persona y se ordenan de menor a mayor cantidad de ingresos
#     personas_ingresos_df = viajes_didier_df.groupBy("identificador").sum("Ingreso_por_Viaje")
#     personas_ingresos_df = personas_ingresos_df.orderBy(col("sum(Ingreso_por_Viaje)").asc(),col("identificador").asc())
         
#     #si se env??a un valor de percentil menor a 0, establece el valor en 0 (como valor m??nimo)
#     if (percentil < 0):
#         percentil = 0

#     #si se env??a un valor de percentil mayor a 100, establece el valor en 100 (como valor m??ximo)
#     if (percentil > 100):
#         percentil = 100        

#     metrica = "percentil_" + str(percentil) 
#     #obtiene el percentil respectivo
#     valor_percentil_df = personas_ingresos_df.select(percentile_approx("sum(Ingreso_por_Viaje)", [percentil/100])[0].alias("Valor"))
#     valor_percentil_df = valor_percentil_df.withColumn("Tipo_de_Metrica",lit(metrica))
#     valor_percentil_df = valor_percentil_df.select (col("Tipo_de_Metrica"), col("Valor"))
    
#     return valor_percentil_df    

# def obtener_metrica_codigo_postal_origen_con_mas_ingresos (viajes_didier_df):
    
#     #excluye los registros con kil??metros o precio_kilometro en 0, negativos o en null
#     viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.kilometros > 0)
#     viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.precio_kilometro > 0)
#     #excluye c??digos postales origen inv??lidos  (0, negativo, null)
#     viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.codigo_postal_origen > 0)

#     viajes_didier_df = viajes_didier_df.withColumn("Ingreso_por_Viaje",col("kilometros")*col("precio_kilometro"))
    
#     #obtiene la cantidad total de ingresos por cada codigo_postal_origen
#     total_ingresos_por_codigo_postal_origen_df = viajes_didier_df.groupBy("codigo_postal_origen").sum("Ingreso_por_Viaje")

#     #obtiene el codigo_postal_origen con m??s ingresos
#     codigo_postal_origen_con_mas_ingresos_df = total_ingresos_por_codigo_postal_origen_df.orderBy(col("sum(Ingreso_por_Viaje)").desc(),col("codigo_postal_origen").asc()).limit(1)

#     codigo_postal_origen_con_mas_ingresos_df = codigo_postal_origen_con_mas_ingresos_df.withColumn("Tipo_de_Metrica",lit("codigo_postal_origen_con_mas_ingresos"))
#     codigo_postal_origen_con_mas_ingresos_df = codigo_postal_origen_con_mas_ingresos_df.select(col("Tipo_de_Metrica"),col("codigo_postal_origen").alias("Valor"))

#     return codigo_postal_origen_con_mas_ingresos_df

# def obtener_metrica_codigo_postal_destino_con_mas_ingresos (viajes_didier_df):

#     #excluye los registros con kil??metros o precio_kilometro en 0, negativos o en null
#     viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.kilometros > 0)
#     viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.precio_kilometro > 0)
#     #excluye c??digos postales origen inv??lidos  (0, negativo, null)
#     viajes_didier_df = viajes_didier_df.filter(viajes_didier_df.codigo_postal_destino > 0)

#     viajes_didier_df = viajes_didier_df.withColumn("Ingreso_por_Viaje",col("kilometros")*col("precio_kilometro"))
    
#     #obtiene la cantidad total de ingresos por cada codigo_postal_destino
#     total_ingresos_por_codigo_postal_destino_df = viajes_didier_df.groupBy("codigo_postal_destino").sum("Ingreso_por_Viaje")

#     #obtiene el codigo_postal_destino con m??s ingresos
#     codigo_postal_destino_con_mas_ingresos_df = total_ingresos_por_codigo_postal_destino_df.orderBy(col("sum(Ingreso_por_Viaje)").desc(),col("codigo_postal_destino").asc()).limit(1)

#     codigo_postal_destino_con_mas_ingresos_df = codigo_postal_destino_con_mas_ingresos_df.withColumn("Tipo_de_Metrica",lit("codigo_postal_destino_con_mas_ingresos"))
#     codigo_postal_destino_con_mas_ingresos_df = codigo_postal_destino_con_mas_ingresos_df.select(col("Tipo_de_Metrica"),col("codigo_postal_destino").alias("Valor"))

#     return codigo_postal_destino_con_mas_ingresos_df    

# #funci??n que une los dataframes de cada una de las m??tricas
# def unir_dataframes_metricas(metrica_persona_con_mas_kilometros_df, metrica_persona_con_mas_ingresos_df, metrica_valor_percentil_25_df, metrica_valor_percentil_50_df, metrica_valor_percentil_75_df, metrica_codigo_postal_origen_con_mas_ingresos_df, metrica_codigo_postal_destino_con_mas_ingresos_df):
    
#     metricas_df = metrica_persona_con_mas_kilometros_df.union(metrica_persona_con_mas_ingresos_df)
#     metricas_df = metricas_df.union(metrica_valor_percentil_25_df)
#     metricas_df = metricas_df.union(metrica_valor_percentil_50_df)
#     metricas_df = metricas_df.union(metrica_valor_percentil_75_df)
#     metricas_df = metricas_df.union(metrica_codigo_postal_origen_con_mas_ingresos_df)
#     metricas_df = metricas_df.union(metrica_codigo_postal_destino_con_mas_ingresos_df)

#     return metricas_df       