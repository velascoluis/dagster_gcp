import re
from pandas import DataFrame
from operator import add
from pyspark import SparkConf, SparkContext, SQLContext


def get_keyval(row):

    text = row.title
    text = re.sub("\\W", " ", text)
    words = text.lower().split(" ")
    return [[w, 1] for w in words]


def get_counts(df):

    df.show(2, False)
    mapped_rdd = df.rdd.flatMap(lambda row: get_keyval(row))
    counts_rdd = mapped_rdd.reduceByKey(add)
    word_count = counts_rdd.collect()
    for e in word_count:
        print(e)
    return word_count

def process_json(filename, sparkcontext):

    sqlContext = SQLContext(sparkcontext)
    df = sqlContext.read.json(filename).select("title")
    output_list = get_counts(df)
    columns = ["token","count"]
    output_df = sqlContext.createDataFrame(output_list,columns)
    output_df.write.mode('overwrite').parquet(filename.replace(".json",".parquet"))


if __name__ == "__main__":
    bucket_name="<ADD_BUCKET_NAME>"
    filename = f"gs://{bucket_name}/stack_overflow000000000000.json"
    conf = (SparkConf()
            .setMaster("local[20]")
            .setAppName("dagster")
            .set("spark.executor.memory", "2g"))

    sc = SparkContext(conf=conf)
    process_json(filename, sc)
