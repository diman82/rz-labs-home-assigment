from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import lit
from pyspark.sql.functions import when
import findspark


def shakespeare_word_count():
    findspark.init()
    conf = SparkConf().setAppName('MyFirstStandaloneApp')
    sc = SparkContext(conf=conf)

    print("loading text file")
    text_file = sc.textFile("./shakespeare.txt")

    counts = text_file.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b)

    print("Number of elements: " + str(counts.count()))
    counts.saveAsTextFile("./shakespeareWordCount")


def data_loading_and_structuring():
    findspark.init()

    spark = SparkSession \
        .builder \
        .appName('rz-labs-home-assigment') \
        .getOrCreate()

    print("loading text file")
    dataset_df = spark.read.option("badRecordsPath", "./badRecordsPath").csv("./dataset.csv", header=False)
    metadata_df = spark.read.option("badRecordsPath", "./badRecordsPath").csv("./metadata.csv", header=True)

    metadata_df = metadata_df.withColumn("plc_name", \
                                         when((metadata_df.machine_type == 'excavator'), lit("PLC303")) \
                                         .when((metadata_df.machine_type == 'wheel trencher'), lit("PLC0120")) \
                                         .when((metadata_df.machine_type == 'generator'), lit("PLC0240")) \
                                         .when((metadata_df.machine_type == 'front loader'), lit("PLC970")) \
                                         .when((metadata_df.machine_type == 'scraper'), lit("PLC030")) \
                                         .otherwise(lit("PLC303")))

    # rename default given column names
    dataset_df = dataset_df.withColumnRenamed("_c0", "name") \
        .withColumnRenamed("_c1", "time") \
        .withColumnRenamed("_c2", "value")

    split_col = f.split("name", '\.')
    dataset_df = dataset_df.withColumn('site_name', split_col.getItem(0))
    dataset_df = dataset_df.withColumn('plc_name', split_col.getItem(1))
    dataset_df = dataset_df.withColumn('sub_system_name', split_col.getItem(2))
    dataset_df = dataset_df.withColumn('reading_type', split_col.getItem(3))
    dataset_df = dataset_df.withColumn('reading_attribute', split_col.getItem(4))

    cond = [dataset_df.plc_name == metadata_df.plc_name]
    inner_join_df = dataset_df \
        .join(metadata_df, cond, 'inner')

    # rename duplicate column names: https://stackoverflow.com/questions/52624888/how-to-write-dataframe-with-duplicate-column-name-into-a-csv-file-in-pyspark
    newNames = ['name', 'time', 'value', 'site_name', 'plc_name', 'sub_system_name', 'reading_type', 'reading_attribute',
                'machine_type', 'machine_vendor', 'machine_serial_number', 'machine_production_date', 'plc_name_duplicate']
    inner_join_df = inner_join_df.toDF(*newNames)

    print(dataset_df.take(10))
    print(metadata_df.take(10))
    print(inner_join_df.take(10))
    inner_join_df.collect()

    inner_join_df.coalesce(1).write.mode('overwrite') \
        .option('header', 'true') \
        .parquet("./mnt/result.parquet")

    # inner_join_df.coalesce(1).write.mode('overwrite')\
    #     .format('com.databricks.spark.csv')\
    #     .option('header', 'true')\
    #     .csv('/tmp/result.csv')  # usually would like to write dataframe to HDFS. but here I write to /tmp which is mapped to ram


if __name__ == "__main__":
    data_loading_and_structuring()
