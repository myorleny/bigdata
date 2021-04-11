from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType, FloatType, StructField,
                               StructType, TimestampType, StringType, DateType)
from pyspark.sql.functions import col, date_format, udf, rank, lit                              
from pyspark.sql.window import Window
 
from pyspark.sql.functions import explode                               



spark = SparkSession.builder.appName("Viajes").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

import sys

def CargarArchivosJSON():
    args = sys.argv
    #Crea un dataframe para ir acumulando los viajes de cada persona
    viajes_schema = StructType([StructField('identificador', IntegerType()),
                            StructField('codigo_postal_origen', IntegerType()),
                            StructField('codigo_postal_destino', IntegerType()),
                            StructField('kilometros', FloatType()),
                            StructField('precio_kilometro', FloatType()),
                            ])

    viajes_didier_df = spark.createDataFrame([], viajes_schema)
    
    #Carga los datos de cada identificador en el dataframe recién creado
    for arg in args:
        if arg[-5:] == ".json":
            print(arg)
            viajes_por_persona_df = spark.read.option("multiline","true").json(arg)
            viajes_por_persona_df = viajes_por_persona_df.withColumn("viajes", explode(viajes_por_persona_df.viajes))
            #viajes_por_persona_df.show()
            viajes_didier_df = viajes_didier_df.union(viajes_por_persona_df.select("identificador", "viajes.codigo_postal_origen", "viajes.codigo_postal_destino", "viajes.kilometros","viajes.precio_kilometro"))
            
    viajes_didier_df.show()

CargarArchivosJSON()    