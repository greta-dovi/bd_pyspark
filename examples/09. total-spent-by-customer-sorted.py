import os
import sys
from pyspark import SparkConf, SparkContext

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

conf = SparkConf().setMaster("local").setAppName("SpendByCustomerSorted")
sc = SparkContext(conf = conf)

def extractCustomerPricePairs(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

input = sc.textFile("customer-orders.csv")
mappedInput = input.map(extractCustomerPricePairs)
totalByCustomer = mappedInput.reduceByKey(lambda x, y: x + y)

#Changed for Python 3 compatibility:
#flipped = totalByCustomer.map(lambda (x,y):(y,x))
flipped = totalByCustomer.map(lambda x: (x[1], x[0]))

totalByCustomerSorted = flipped.sortByKey()

results = totalByCustomerSorted.collect()
for result in results:
    print(result)
