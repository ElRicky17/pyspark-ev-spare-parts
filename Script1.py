from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F

def parseInput(line):
    fields = line.split('\t')
    return Row(id_repuesto=int(fields[0]), sku_interno=str(fields[1]), nombre=str(fields[2]), categoria=str(fields[3]), marca=str(fields[4]))

if __name__ == "__main__":

    spark = SparkSession.builder.appName("repuestosCategoria").getOrCreate()
    lines = spark.sparkContext.textFile("hdfs://namenode:9000/user/root/input/REPUESTO.tsv")
    dataRDD = lines.map(parseInput)
    df = spark.createDataFrame(dataRDD).cache() 

    repuestosCategoria = df.groupBy("categoria") \
        .agg(F.count("*").alias("cantidadRepuestos")) \
        .orderBy(F.desc("cantidadRepuestos"))

    results = repuestosCategoria.collect()

    for row in results:
        print(f"Respuesto: {row['categoria']} | Cantidad: {row['cantidadRepuestos']}")

    spark.stop()