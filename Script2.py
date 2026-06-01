from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F

def parseInput(line):
    fields = line.split('\t')
    return Row(id_bodega=int(fields[0]), id_repuesto=int(fields[1]), stock_disponible=int(fields[2]), stock_reservado=int(fields[3]), punto_reorden=int(fields[4]),costo_promedio=float(fields[5]))

if __name__ == "__main__":

    spark = SparkSession.builder.appName("stockBodega").getOrCreate()
    lines = spark.sparkContext.textFile("hdfs://namenode:9000/user/root/input/INVENTARIO.tsv")
    dataRDD = lines.map(parseInput)
    df = spark.createDataFrame(dataRDD).cache() 

    stockPorBodega = df.groupBy("id_bodega") \
        .agg(F.sum("stock_disponible").alias("stock_total")) \
        .orderBy(F.asc("id_bodega"))

    results = stockPorBodega.collect()

    for row in results:
        print(f"Bodega: {row['id_bodega']} | Stock Total: {row['stock_total']}")

    spark.stop()