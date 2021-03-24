from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType, FloatType, StructField,
                               StructType, TimestampType, StringType, DateType)

spark = SparkSession.builder.appName("Read Transactions").getOrCreate()

# Une los datos de los tres archivos
def unir_dataframes(ciclista_df, ruta_df, actividad_df):
    
    ciclista_actividad_df = ciclista_df.join(actividad_df, ciclista_df.cedula == actividad_df.cedula_Ciclista, "left_outer")

    ciclista_actividad_ruta_df = ciclista_actividad_df.join(ruta_df, ciclista_actividad_df.codigo_Ruta == ruta_df.codigo , "left_outer")
  
    return ciclista_actividad_ruta_df



def programaPrincipal():

    ciclista_schema = StructType([StructField('cedula', IntegerType()),
                            StructField('nombre_Completo', StringType()),
                            StructField('provincia', StringType()),
                            ])

    ciclista_df = spark.read.csv("ciclista.csv",
                            schema=ciclista_schema,
                            header=False)

    ciclista_df.show()

    ruta_schema = StructType([StructField('codigo', IntegerType()),
                            StructField('nombre_Ruta', StringType()),
                            StructField('kilometros', FloatType()),
                            ])

    ruta_df = spark.read.csv("ruta.csv",
                            schema=ruta_schema,
                            header=False)

    ruta_df.show()

    actividad_schema = StructType([StructField('codigo_Ruta', IntegerType()),
                            StructField('cedula_Ciclista', IntegerType()),
                            StructField('fecha', DateType()),
                            ])

    actividad_df = spark.read.csv("actividad.csv",
                            schema=actividad_schema,
                            header=False)

    actividad_df.show()    

    ciclista_actividad_ruta_df = unir_dataframes(ciclista_df, ruta_df, actividad_df)

    ciclista_actividad_ruta_df.show()


programaPrincipal()


