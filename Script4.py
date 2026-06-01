
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F

def parseInput(line):
    fields = line.split('\t')
    return Row(id_orden=int(fields[0]), id_proveedor=int(fields[1]), fecha_pedido=str(fields[2]), estado=str(fields[3]))

if __name__ == "__main__":

    spark = SparkSession.builder.appName("cantidadPorEstado").getOrCreate()
    lines = spark.sparkContext.textFile("hdfs://namenode:9000/user/root/input/ORDEN_COMPRA.tsv")
    dataRDD = lines.map(parseInput)
    df = spark.createDataFrame(dataRDD).cache() 

    cantidadXEstado = df.groupBy("estado") \
        .agg(F.count("*").alias("totalEstado")) \
        .orderBy(F.desc("totalEstado"))

    results = cantidadXEstado.collect()

    for row in results:
        print(f"Estado: {row['estado']} | Cantidad Total: {row['totalEstado']}")

    spark.stop()