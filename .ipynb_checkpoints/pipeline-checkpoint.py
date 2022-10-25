#reomve when pushing to production
import findspark
findspark.init()
from pyspark.sql import functions as f, SparkSession,Row
import json,os
import path_clm as pc



path = pc.Path()

# def config():
#     with open(path.config) as f:
#         return json.loads(f.read())

config = path.config()

spark = (SparkSession
        .builder
        .appName("CaseStudy - MovieLenz")
        .master("local[6]").enableHiveSupport()
        .getOrCreate())


def caseStudy():
    
    SourceMacroDict = {}

    SourceMacroDict['path'] = config['paths']['source_dir']

    ##creating data frames
    df = {}

    df['genome-scores'] = spark.table("genome_scores") # loading data from hive

    #loading data from dynamodb
    df['genome-tags'] = (
        spark.read.format("dynamodb").option("throughput",100000)
                        .option("region","ap-south-1")
                        .option("tableName","genome_tags").load()
    )

    df['movies'] = (
        spark.read.format("dynamodb").option("throughput",100000)
                        .option("region","ap-south-1")
                        .option("tableName","movies").load()
    )

    df['ratings'] = (
        spark.read.format("dynamodb").option("throughput",100000)
                        .option("region","ap-south-1")
                        .option("tableName","ratings").load().drop("ratings_row")
    )


    df['ratings'] = df['ratings'].withColumn("ratings_timestamp", f.expr("to_timestamp(from_unixtime(timestamp))")).drop("timestamp")

    df['movies'] = df['movies'].withColumn("releasedInyear",f.expr("cast(substr(split(title,'[(]')[1],1,4) as Integer)"))

    #final report 
    df['final_report'] = (
        df['genome-scores'].where("relevance > 0.5").join(df['genome-tags'],['tagid'],'inner')
        .join(
            df['movies'].where("releasedInyear is not null") ,['movieID'],'inner'
        )
        .join(
            df['ratings'].groupBy("movieId").agg(f.round(f.avg("rating"),2).alias("total_ratings")),['movieId'],'inner'
        )
    )
    #writing intermediate result to S3
    df['final_report'].write.parquet(path.s3_path+"movielenz_final",mode="overwrite")

    df['final_report'] = spark.read.parquet(path.s3_path+"movielenz_final",mode="overwrite")


    # query to report movie name year adn total ratings along with geners
    df['movie_report'] = (
        df['final_report'].groupBy("movieId","title").agg(f.round(f.avg("total_ratings"),2).alias("total_ratings"),
        f.first("releasedinyear").alias("releasedinyear"),f.first("genres").alias("genres") )
        .withColumn("total_genres_covered", f.expr("size(split(genres,'[|]'))"))
    )

    # writng movie_report to mysql.
    (
        df['movie_report']
        .write.format("jdbc")
        .option("url",config['mysql']['url'])
        .option("user",config['mysql']['user'])
        .option("password",config['mysql']['password'])
        .option("batchSize",10000)
        .option("dbtable","movie_report")
        .option("driver","com.mysql.cj.jdbc.Driver")
        .mode("overwrite")
        .save()
    )

if __name__=="__main__":
    
    try:
        caseStudy()
        print("success")
    except Exception as e:
        print(e)