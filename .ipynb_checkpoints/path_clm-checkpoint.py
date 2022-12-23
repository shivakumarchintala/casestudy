import os,json
class Path:
    mysql = r"/home/hdoop/spark-3.2.2-bin-hadoop3.2/jars/mysql-connector-java-8.0.29.jar"
    s3_path = "s3a://movielenzreports/casestudy/"
    def config(self):
        config  = r"/config.json"
        with open(os.getcwd()+config) as f:
            return json.loads(f.read())
# spark.sparkContext._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
# spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
# spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", \
#                                      "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
# spark.sparkContext._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")

# spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key","AKIA4PAAYW6P3RJJN27T")
# spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key","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")
# spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
