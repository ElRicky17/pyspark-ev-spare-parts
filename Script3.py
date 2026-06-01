from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F

def parseInput(line):
    fields = line.split('\t')
    return Row(id_detalle_venta=int(fields[0]), id_factura=int(fields[1]), id_bodega=int(fields[2]), id_repuesto=int(fields[3]), cantidad=int(fields[4]),precio_unitario=float(fields[5]))

if __name__ == "__main__":

    spark = SparkSession.builder.appName("ventasPorRepuesto").getOrCreate()
    lines = spark.sparkContext.textFile("hdfs://namenode:9000/user/root/input/DETALLE_VENTA.tsv")
    dataRDD = lines.map(parseInput)
    df = spark.createDataFrame(dataRDD).cache() 

    ventasXRepuesto = df.groupBy("id_repuesto") \
        .agg(F.sum("cantidad").alias("cantidaTotal")) \
        .orderBy(F.desc("cantidaTotal"))

    results = ventasXRepuesto.collect()

    for row in results:
        print(f"Repuesto: {row['id_repuesto']} | Cantidad Total: {row['cantidaTotal']}")

    spark.stop()