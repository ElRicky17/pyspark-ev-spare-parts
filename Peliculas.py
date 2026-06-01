from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F

def loadMovieNames():
    movieNames = {}
    with open("/tmp/u.item", encoding="ISO-8859-1") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames
def parseInput(line):
    fields = line.split()
    return Row(userID=int(fields[0]), movieID=int(fields[1]), rating=float(fields[2]))

if __name__ == "__main__":

    spark = SparkSession.builder.appName("TopUsersTopMovies").getOrCreate()
    movieNames = loadMovieNames()
    lines = spark.sparkContext.textFile("hdfs://namenode:9000/user/root/input/u.data")
    dataRDD = lines.map(parseInput)
    df = spark.createDataFrame(dataRDD).cache() 

    topFiftyMovies = df.groupBy("movieID") \
        .count() \
        .orderBy(F.desc("count")) \
        .limit(50)

    dfFiltered = df.join(topFiftyMovies, "movieID", "inner")

    topTenUsers = dfFiltered.groupBy("userID") \
        .agg(F.count("movieID").alias("vistas")) \
        .orderBy(F.desc("vistas")) \
        .limit(10)

    print("\nTop 10 Usuarios que mas vieron el top 59 peliculas:")
    results = topTenUsers.collect()
    
    for row in results:
        print(f"Usuario ID: {row['userID']} | Películas vistas del Top 50: {row['vistas']}")

    spark.stop()