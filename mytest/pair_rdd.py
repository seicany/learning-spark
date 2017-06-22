from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)

rdd = sc.textFile("test1.py")
words = rdd.flatMap(lambda x: x.split(" "))

# 方式1
result = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
print result.collect()

# 方式2
result = words.countByValue()
print result
