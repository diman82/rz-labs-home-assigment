from pyspark.sql import SparkSession
import findspark


def data_publishing():
    findspark.init()
    findspark.find()

    spark = SparkSession \
        .builder \
        .appName('rz-labs-home-assigment') \
        .getOrCreate()

    dataset_df = spark.read\
        .option("badRecordsPath", "./badRecordsPath")\
        .parquet("./mnt/result.parquet/")

    # write to some sink (csv in this example)
    dataset_df.coalesce(1).write.mode('overwrite').option('header', 'true').csv('./mnt/result.csv')


if __name__ == "__main__":
    data_publishing()
