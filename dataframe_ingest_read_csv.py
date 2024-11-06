from pyspark.sql.types import *
from pyspark.sql import functions as F

arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"

despachantes = spark.read.csv("/home/diegocarloni/download/despachantes.csv", header=False, schema=arqschema)

despachantes.show()

despachantes.select("id", "nome", "vendas").where(F.col("vendas") > 20).show()

despachantes.select("id", "nome", "vendas").where((F.col("vendas") > 20 ) & (F.col("vendas") < 40)).show()

despachantes.select(F.year("data")).show()
despachantes.select(F.year("data")).distinct().show()
despachantes.select("nome", F.year("data")).orderBy("nome").show()
despachantes.select("data").groupBy(F.year("data")).count().show()
despachantes.select(F.sum("vendas")).show()

novodf = despachantes.withColumnRenamed("nome", "nomes")
novodf.columns

despachantes2 = despachantes.withColumn("data2", F.to_timestamp(F.col("data"), "yyyy-MM-dd"))

despachantes2.schema

# Modifica tipos de colunas específicas
despachantes3 = despachantes.withColumn("data", F.col("data").cast("date"))

despachantes3.schema

despachantes4 = despachantes3.selectExpr("id", "nome", "status", "cidade", "cast(vendas as double) vendas", "cast(data as timestamp) data")

despachantes4.schema
