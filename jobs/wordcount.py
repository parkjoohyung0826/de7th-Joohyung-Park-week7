from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col


spark = (
    SparkSession.builder
    .appName("WordCount_ParkJoohyung")
    .getOrCreate()
)

df = spark.read.text("/opt/spark/data/wordcount.txt")

words = df.select(
    explode(split(col("value"), " ")).alias("word")
)

word_counts = (
    words
    .filter(col("word") != "")
    .groupBy("word")
    .count()
    .orderBy(col("count").desc())
)

word_counts.show(20, truncate=False)

spark.stop()

