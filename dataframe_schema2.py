from pyspark.sql.functions import sum
from pyspark.sql.functions import expr

schema2 = "Produtos STRING, Vendas INT"
vendas = [["Produto A", 100], ["Produto B", 200], ["Produto A", 300]]
df3 = spark.createDataFrame(vendas, schema2)
df3.show()

#propriedades do dataframe
df3.schema 
df3.columns

df3.printSchema()

df3.select("Produtos").show()
df3.select("Produtos", "Vendas").show()
df3.select("Vendas","Produtos").show()

df3.select("Produtos", "Vendas", expr("Vendas * 0.2")).show()

df3.groupBy("Produtos").agg(sum("Vendas")).show()

agrupado = df3.groupBy("Produtos").agg(sum("Vendas"))
agrupado.show()

agrupado.select("Produtos").show()
agrupado.selectExpr(["Produtos as Produtos", "`sum(Vendas)` as Total"]).show()

agrupado.schema
