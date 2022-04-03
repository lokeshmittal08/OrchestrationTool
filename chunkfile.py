import pandas as pd

chunk_size=1000
batch_no=1

for chunk in pd.read_csv('movie_rating.csv',chunksize=chunk_size):
    chunk.to_csv('movie_rating'+str(batch_no)+'.csv',index=False)
    batch_no+=1

#
# from pyspark.sql import SparkSession
# spark=SparkSession.builder.appName("Read AWS  S3 & Write to S3").master("local[*]").getOrCreate()
# spark._jsc.hadoopConfiguration().set("fs.s3a.access.key","")
# spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key","")
# spark._jsc.hadoopConfiguration().set("fs.s3a.amazonnews.com","s3.amazonaws.com")
#
# Reading from S3
# myDf=spark.read.format("csv").option("header","true").option("inferSchema","true").csv("s3a://saif-bigdata-demo/")
#
# myDf.printSchema()
# myDf.show(truncate=False)
#Writing to S3 using pyspark
# df.write.format("csv").mode("oyverwrite").save("s3a://saif-bigdata-demo/opt_emp_mgr_cnt")