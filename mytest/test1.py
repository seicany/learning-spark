from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)
nums = sc.parallelize([1, 2, 3, 4])
print nums.collect()
sumCount = nums.aggregate((0, 0),
                          lambda acc, value: (acc[0] + value, acc[1] + 1),
                          lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])
                          )

print sumCount
