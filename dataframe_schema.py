from pyspark.sql import SparkSession

df1 = spark.createDataFrame([("Pedro", 10), ("Maria", 20), ('José', 40)])

df1.show()

schema = "Id INT, Nome STRING"
dados = [[1, "Pedro"], [2, "Maria"], [3, "José"]]
df2 = spark.createDataFrame(dados, schema)
df2.show()


