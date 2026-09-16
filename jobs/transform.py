import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, trim, expr


if len(sys.argv) >= 2:
    threshold = int(sys.argv[1])
else:
    threshold = 2015


spark = (
    SparkSession.builder
    .appName("NetflixTransform")
    .getOrCreate()
)


input_path = "/opt/airflow/data/netflix_titles.csv"
output_path = "/opt/airflow/data/silver_output"


# CSV 읽기
df = (
    spark.read
    .option("header", True)
    .option("multiLine", True)
    .option("quote", '"')
    .option("escape", '"')
    .option("mode", "PERMISSIVE")
    .csv(input_path)
)


# release_year를 안전하게 숫자로 변환 후 threshold 이상만 필터
filtered = (
    df
    .withColumn(
        "release_year_int",
        expr("try_cast(release_year as int)")
    )
    .filter(col("release_year_int") >= threshold)
)


# listed_in의 장르를 쉼표 기준으로 분리 → explode
exploded = (
    filtered
    .withColumn(
        "genre",
        explode(split(col("listed_in"), ","))
    )
    .withColumn(
        "genre",
        trim(col("genre"))
    )
)


# type × genre 별 개수 집계
result = (
    exploded
    .groupBy("type", "genre")
    .count()
)


result.show(50, truncate=False)


row_count = result.count()

print(f"Aggregated row count: {row_count}")


# Snappy Parquet 저장
(
    result.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(output_path)
)


spark.stop()