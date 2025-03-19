from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
import pandas as pd

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("CSV Sampling") \
    .config("spark.driver.memory", "6g")\
    .getOrCreate()

# [1] Kinder ----

# Define the Schema
schema = StructType([
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("mrun", IntegerType(), True),
    StructField("sex_stud", IntegerType(), True),
    StructField("birth_date_stud", IntegerType(), True),
    StructField("school_id", IntegerType(), True),
    StructField("RBD", FloatType(), True),
    StructField("ID_ESTAB_J", FloatType(), True),
    StructField("ID_ESTAB_I", FloatType(), True),
    StructField("NOM_ESTAB", StringType(), True),
    StructField("COD_REG_ESTAB", IntegerType(), True),
    StructField("COD_PRO_ESTAB", IntegerType(), True),
    StructField("COD_COM_ESTAB", IntegerType(), True),
    StructField("NOM_REG_ESTAB", StringType(), True),
    StructField("NOM_REG_A_ESTAB", StringType(), True),
    StructField("NOM_PRO_ESTAB", StringType(), True),
    StructField("NOM_COM_ESTAB", StringType(), True),
    StructField("COD_DEPROV_ESTAB", IntegerType(), True),
    StructField("NOM_DEPROV_ESTAB", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("rural_establishment", IntegerType(), True),
    StructField("ORIGEN", IntegerType(), True),
    StructField("DEPENDENCIA", IntegerType(), True),
    StructField("NIVEL1", IntegerType(), True),
    StructField("NIVEL2", IntegerType(), True),
    StructField("COD_ENSE1_M", FloatType(), True),
    StructField("COD_GRADO_M", FloatType(), True),
    StructField("LET_CUR_M", StringType(), True),
    StructField("COD_TIP_CUR_M", FloatType(), True),
    StructField("COD_DEPE1_M", FloatType(), True),
    StructField("COD_ENSE2_M", FloatType(), True),
    StructField("ESTADO_ESTAB_M", FloatType(), True),
    StructField("CORR_GRU_J", FloatType(), True),
    StructField("COD_PROG_J", FloatType(), True),
    StructField("DESC_PROG_J", StringType(), True),
    StructField("COD_NIVEL_J", FloatType(), True),
    StructField("DESC_NIVEL_J", StringType(), True),
    StructField("COD_MODAL_J", FloatType(), True),
    StructField("DESC_MODAL_J", StringType(), True),
    StructField("COD_JOR_J", FloatType(), True),
    StructField("NOM_JOR_J", StringType(), True),
    StructField("DESC_MOD_I", StringType(), True),
    StructField("DESC_NIV_I", StringType(), True),
    StructField("COD_NIVEL_I", FloatType(), True),
    StructField("COD_GRUPO_I", FloatType(), True),
    StructField("TIPO_SOSTENEDOR", IntegerType(), True),
    StructField("FORMAL", IntegerType(), True),
    StructField("ASIS_REAL_J", FloatType(), True),
    StructField("ASIS_POTEN_J", FloatType(), True),
    StructField("DIAS_TRAB_GRUPO_J", FloatType(), True)
])

# import the csv file using pyspark
input_path = 'C:/Users/jparedesm/Dropbox/ChileEduc/data/clean/Kinder_.csv'
df = spark.read.csv(input_path, schema=schema, sep=";", header=True)

# do a random sample and keep 2%
sample_df = df.sample(withReplacement=False, fraction=0.02, seed=42)

# convert it to pandas dataframe and write it to a csv file
sample_pandas_df = pd.DataFrame(sample_df.collect(), columns=[field.name for field in sample_df.schema.fields])
output_path = 'C:/Users/jparedesm/Dropbox/ChileEduc/data/clean/Kinder_sample.csv'
sample_pandas_df.to_csv(output_path, index=False, sep=';', encoding='utf-8', mode='w')
print(f"Number of observations in the sample: {sample_pandas_df.shape[0]}")

# [2] Assistance ----
schema = StructType([
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("rbd", IntegerType(), True),
    StructField("DGV_RBD", IntegerType(), True),
    StructField("NOM_RBD", StringType(), True),
    StructField("COD_REG_RBD", IntegerType(), True),
    StructField("NOM_REG_RBD_A", StringType(), True),
    StructField("COD_PRO_RBD", IntegerType(), True),
    StructField("COD_COM_RBD", IntegerType(), True),
    StructField("NOM_COM_RBD", StringType(), True),
    StructField("COD_DEPROV_RBD", IntegerType(), True),
    StructField("NOM_DEPROV_RBD", StringType(), True),
    StructField("RURAL_RBD", IntegerType(), True),
    StructField("COD_DEPE", IntegerType(), True),
    StructField("COD_DEPE2", IntegerType(), True),
    StructField("COD_ENSE", IntegerType(), True),
    StructField("COD_ENSE2", IntegerType(), True),
    StructField("COD_GRADO", IntegerType(), True),
    StructField("LET_CUR", StringType(), True),
    StructField("MRUN", IntegerType(), True),
    StructField("GEN_ALU", IntegerType(), True),
    StructField("FEC_NAC_ALU", IntegerType(), True),
    StructField("COD_COM_ALU", IntegerType(), True),
    StructField("NOM_COM_ALU", StringType(), True),
    StructField("DIAS_ASISTIDOS", IntegerType(), True),
    StructField("DIAS_TRABAJADOS", IntegerType(), True),
    StructField("ASIS_PROMEDIO", StringType(), True)
])

# import the csv file using pyspark
input_path = 'C:/Users/jparedesm/Dropbox/ChileEduc/data/clean/Assistance.csv'
df = spark.read.csv(input_path, schema=schema, sep=";", header=True)

# do a random sample and keep 0.5%
sample_df = df.sample(withReplacement=False, fraction=0.005, seed=42)

# convert it to pandas dataframe and write it to a csv file
sample_pandas_df = pd.DataFrame(sample_df.collect(), columns=[field.name for field in sample_df.schema.fields])
output_path = 'C:/Users/jparedesm/Dropbox/ChileEduc/data/clean/Assistance_sample.csv'
sample_pandas_df.to_csv(output_path, index=False, sep=';', encoding='utf-8', mode='w')
print(f"Number of observations in the sample: {sample_pandas_df.shape[0]}")

# [3] Performance ----
schema = StructType([
    StructField("year", IntegerType(), True),
    StructField("RBD", IntegerType(), True),
    StructField("DGV_RBD", IntegerType(), True),
    StructField("NOM_RBD", StringType(), True),
    StructField("COD_REG_RBD", IntegerType(), True),
    StructField("NOM_REG_RBD_A", StringType(), True),
    StructField("COD_PRO_RBD", IntegerType(), True),
    StructField("COD_COM_RBD", IntegerType(), True),
    StructField("NOM_COM_RBD", StringType(), True),
    StructField("COD_DEPROV_RBD", IntegerType(), True),
    StructField("NOM_DEPROV_RBD", StringType(), True),
    StructField("COD_DEPE", IntegerType(), True),
    StructField("COD_DEPE2", IntegerType(), True),
    StructField("RURAL_RBD", IntegerType(), True),
    StructField("ESTADO_ESTAB", IntegerType(), True),
    StructField("COD_ENSE", IntegerType(), True),
    StructField("COD_ENSE2", IntegerType(), True),
    StructField("COD_GRADO", IntegerType(), True),
    StructField("LET_CUR", StringType(), True),
    StructField("COD_JOR", IntegerType(), True),
    StructField("COD_TIP_CUR", IntegerType(), True),
    StructField("COD_DES_CUR", IntegerType(), True),
    StructField("MRUN", IntegerType(), True),
    StructField("GEN_ALU", IntegerType(), True),
    StructField("FEC_NAC_ALU", StringType(), True),
    StructField("EDAD_ALU", IntegerType(), True),
    StructField("COD_REG_ALU", IntegerType(), True),
    StructField("COD_COM_ALU", IntegerType(), True),
    StructField("NOM_COM_ALU", StringType(), True),
    StructField("COD_RAMA", IntegerType(), True),
    StructField("COD_SEC", IntegerType(), True),
    StructField("COD_ESPE", IntegerType(), True),
    StructField("PROM_GRAL", StringType(), True),
    StructField("ASISTENCIA", IntegerType(), True),
    StructField("SIT_FIN", StringType(), True),
    StructField("SIT_FIN_R", StringType(), True),
    StructField("COD_MEN", IntegerType(), True)
])
# import the csv file using pyspark
input_path = 'C:/Users/jparedesm/Dropbox/ChileEduc/data/clean/Performance.csv'
df = spark.read.csv(input_path, schema=schema, sep=";", header=True)

# do a random sample and keep 2%
sample_df = df.sample(withReplacement=False, fraction=0.02, seed=42)

# convert it to pandas dataframe and write it to a csv file
sample_pandas_df = pd.DataFrame(sample_df.collect(), columns=[field.name for field in sample_df.schema.fields])
output_path = 'C:/Users/jparedesm/Dropbox/ChileEduc/data/clean/Performance_sample.csv'
sample_pandas_df.to_csv(output_path, index=False, sep=';', encoding='utf-8', mode='w')
print(f"Number of observations in the sample: {sample_pandas_df.shape[0]}")

# Detener la sesión de Spark
spark.stop()
