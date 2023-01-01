
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pandas as pd
spark = SparkSession.builder.getOrCreate();

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.13.0,org.apache.spark:spark-sql-kafka-0-10_2.11:2.13.0 pyspark-shell' 
from ast import literal_eval


filepath = "/home/gildas/Bureau/ECE/data/2Tour2022.csv"
tab = []
df = pd.read_csv(filepath,encoding="latin1", sep=";")
tab = df.columns.to_list()

names = []
i = 28
x = 28
while x < 35 : 
    names.append("_c"+str(i))
    i = i + 1
    x = x + 1

schematemp = tab + names
schema = T.StructType()
for x in schematemp: 
    schema.add(x, T.StringType(), True)

df = spark.read.option("encoding", "latin1").schema(schema).options(delimiter=";").csv(filepath)
df = df.withColumnRenamed("Code du b.vote","Code du b_vote")
df.show()


delete = []

df = df.drop(df.colRegex("`^*%*`"))
for col in df.columns : 
  if "%" in col : delete.append(col)
  if "Libellé" in col : delete.append(col)

for y in delete :
  df = df.drop(y)

df = df.drop("_c28","_c29","_c33","_c34")  

df.show()



df = df.withColumn("Code", F.concat( F.col("Code du département"), F.lit("/"),\
                                     F.col("Code de la circonscription"), F.lit("/"),\
                                     F.col("Code de la commune"), F.lit("/"),\
                                     F.col("Code du b_vote")))\
       .withColumnRenamed("Code","Code1")

df = df.drop("Code du département","Code de la circonscription","Code de la commune","Code du b_vote")
df.show()
df0 = df.select("Code1","Nom","Prénom","Voix")
df1 = df.select("Code1","_c30","_c31","_c32")\
        .withColumnRenamed("_c30","Nom").withColumnRenamed("_c31","Prénom").withColumnRenamed("_c32","Voix")

dfnew = df0.union(df1)
dfnew.count()
df = df.select("Code1","Inscrits","Abstentions","Votants","Blancs","Nuls","Exprimés").withColumnRenamed("Code1","Code")
df = df.join(dfnew, df.Code == dfnew.Code1 ,"inner").drop("Code1")
df.show()
df = df.orderBy(F.col("Code"),F.col("Nom"))
df.show()
df = df.withColumn("Tour", lit(2))\
       .withColumn('Code du département', F.split(F.col("Code"), '/').getItem(0)) \
       .withColumn('Code de la circonscription', F.split(F.col("Code"), '/').getItem(1)) \
       .withColumn('Code de la commune', F.split(F.col("Code"), '/').getItem(2))\
       .withColumn('Code du b_vote', F.split(F.col("Code"), '/').getItem(3))\
       .drop("Code")

df = df.select("Tour","Code du département","Code de la circonscription","Code de la commune","Code du b_vote",\
          "Inscrits","Abstentions","Votants","Blancs","Nuls","Exprimés","Nom","Prénom","Voix")

df.show()

df.write.csv("/home/gildas/Bureau/ECE/data/datatour2")