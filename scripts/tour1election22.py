
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pandas as pd
spark = SparkSession.builder.getOrCreate();

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.13.0,org.apache.spark:spark-sql-kafka-0-10_2.11:2.13.0 pyspark-shell' 
from ast import literal_eval

filepath = "/home/gildas/Bureau/ECE/data/1Tour2022.csv"
tab = []
df = pd.read_csv(filepath,encoding="latin1", sep=";")
tab = df.columns.to_list()

names = []
i = 28
x = 28
while x < 105 : 
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
# Je supprime toutes les columns dont j'ai pas besoins
# On a les libellé dont on a pas besoins vue qu'on a leur code
# Les colonnes avec les pourcentages on peut les calculer par nous même

delete = []

df = df.drop(df.colRegex("`^*%*`"))
for col in df.columns : 
  if "%" in col : delete.append(col)
  if "Libellé" in col : delete.append(col)

for y in delete :
  df = df.drop(y)

# pour supprimer les pourcentages des colonnes unnamed

i = 28
x = 2
while (i < 104) :
  df = df.drop(F.col("_c"+str(i)))
  x = x + 1
  if (x == 4) :
    i = i + 3
    x = 0
  i = i + 1

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
df2 = df.select("Code1","_c37","_c38","_c39")\
        .withColumnRenamed("_c37","Nom").withColumnRenamed("_c38","Prénom").withColumnRenamed("_c39","Voix")
df3 = df.select("Code1","_c44","_c45","_c46")\
        .withColumnRenamed("_c44","Nom").withColumnRenamed("_c45","Prénom").withColumnRenamed("_c46","Voix")
df4 = df.select("Code1","_c51","_c52","_c53")\
        .withColumnRenamed("_c51","Nom").withColumnRenamed("_c52","Prénom").withColumnRenamed("_c53","Voix")
df5 = df.select("Code1","_c58","_c59","_c60")\
        .withColumnRenamed("_c58","Nom").withColumnRenamed("_c59","Prénom").withColumnRenamed("_c60","Voix")
df6 = df.select("Code1","_c65","_c66","_c67")\
        .withColumnRenamed("_c65","Nom").withColumnRenamed("_c66","Prénom").withColumnRenamed("_c67","Voix")
df7 = df.select("Code1","_c72","_c73","_c74")\
        .withColumnRenamed("_c72","Nom").withColumnRenamed("_c73","Prénom").withColumnRenamed("_c74","Voix")
df8 = df.select("Code1","_c79","_c80","_c81")\
        .withColumnRenamed("_c79","Nom").withColumnRenamed("_c80","Prénom").withColumnRenamed("_c81","Voix")
df9 = df.select("Code1","_c86","_c87","_c88")\
        .withColumnRenamed("_c86","Nom").withColumnRenamed("_c87","Prénom").withColumnRenamed("_c88","Voix")
df10 = df.select("Code1","_c93","_c94","_c95")\
        .withColumnRenamed("_c93","Nom").withColumnRenamed("_c94","Prénom").withColumnRenamed("_c95","Voix")
df11 = df.select("Code1","_c100","_c101","_c102")

dfnew = df0.union(df1).union(df2).union(df3).union(df4).union(df5).union(df6).union(df7).union(df8).union(df9).union(df10)
dfnew.count()

df = df.select("Code1","Inscrits","Abstentions","Votants","Blancs","Nuls","Exprimés").withColumnRenamed("Code1","Code")
df = df.join(dfnew, df.Code == dfnew.Code1 ,"inner").drop("Code1")
df.show()
df = df.orderBy(F.col("Code"),F.col("Nom"))
df.show()
df = df.withColumn("Tour", lit(1))\
       .withColumn('Code du département', F.split(F.col("Code"), '/').getItem(0)) \
       .withColumn('Code de la circonscription', F.split(F.col("Code"), '/').getItem(1)) \
       .withColumn('Code de la commune', F.split(F.col("Code"), '/').getItem(2))\
       .withColumn('Code du b_vote', F.split(F.col("Code"), '/').getItem(3))\
       .drop("Code")

df = df.select("Tour","Code du département","Code de la circonscription","Code de la commune","Code du b_vote",\
          "Inscrits","Abstentions","Votants","Blancs","Nuls","Exprimés","Nom","Prénom","Voix")

df.show()
# Add ranking 
# df = df

df.write.csv("/home/gildas/Bureau/ECE/data/datatour1")