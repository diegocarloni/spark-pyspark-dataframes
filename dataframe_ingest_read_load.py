from pyspark.sql.types import *

arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"

desp_autoschema = spark.read.load("/home/diegocarloni/download/despachantes.csv", header=False, format="csv", sep=",", inferSchema=True)

desp_autoschema.show()
desp_autoschema.printSchema()