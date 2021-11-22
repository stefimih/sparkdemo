
# import os
# os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"


from databricks import koalas as ks
import pyspark.pandas as ps
import pyspark

from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .master("local[1]")\
    .appName("SparkByExamples.com")\
    .getOrCreate()
#.builder().master("local[1]").appName("SparkByExamples.com").getOrCreate() 
# from pip._internal.operations import freeze


#in Python
taxiData = spark\
.read\
.option("inferSchema", "true")\
.option("header", "true")\
.csv("train.csv")

print("-----------------------------------------------------------")
print(taxiData.take(3))
print("-----------------------------------------------------------")
taxiData.sort("id").explain()
print("-----------------------------------------------------------")



# spark.conf.set("spark.sql.shuffle.partitions", "5")
print("-----------------------------------------------------------")
print(taxiData.sort("id").take(2))
print("-----------------------------------------------------------")



taxiData.createOrReplaceTempView("taxiData")

# in Python
sqlWay = spark.sql("""
SELECT pickup_datetime, count(1)
FROM taxiData
GROUP BY pickup_datetime
""")

dataFrameWay = taxiData\
.groupBy("pickup_datetime")\
.count()
sqlWay.explain()
dataFrameWay.explain()

print(sqlWay)
print(dataFrameWay)

# x = freeze.freeze()
# for p in x:
#     print(p)


# #Reads entire file into a RDD as single record.
# rdd3 = spark.sparkContext.wholeTextFiles("train.csv")

# # Action - count
# print("Count : "+str(rdd3.count()))

# # Action - first
# firstRec = rdd3.first()
# print("First Record : "+str(firstRec[0]) + ","+ firstRec[1])

# # Action - max
# datMax = rdd3.max()
# print("Max Record : "+str(datMax[0]) + ","+ datMax[1])





# df = ps.read_csv("train.csv")
# df2 = ks.read_csv("train.csv")

# df['sum'] = df.x + df.y + df.z

# print(df)

# print("Hello Minikube!")