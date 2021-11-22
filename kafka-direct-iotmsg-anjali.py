

from __future__ import print_function

import sys
import re

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from operator import add


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka-direct-iotmsg-anjali.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 2)

    sc.setLogLevel("WARN")

    ###############
    # Globals
    ###############
    precipitationTotal = 0.0
    precipitationCount = 0
    precipitationAvg = 0.0

    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    # Read in the Kafka Direct Stream into a TransformedDStream
    lines = kvs.map(lambda x: x[1])
    jsonLines = lines.map(lambda x: re.sub(r"\s+", "", x, flags=re.UNICODE))

    ############
    # Processing
    ############
    # foreach function to iterate over each RDD of a DStream
    def processPrecipitationRDD(time, rdd):
      # Match local function variables to global variables
      global precipitationTotal
      global precipitationCount
      global precipitationAvg

      precipitationList = rdd.collect()
      for precipitationFloat in precipitationList:
        precipitationTotal += float(precipitationFloat)
        precipitationCount += 1
        precipitationAvg = precipitationTotal / precipitationCount
      print("Precipitation Total = " + str(precipitationTotal))
      print("Precipitation Count = " + str(precipitationCount))
      print("Avg Precipitation = " + str(precipitationAvg))

    # Search for specific IoT data values (assumes jsonLines are split(','))
    precipitationValues = jsonLines.filter(lambda x: re.findall(r"precipitation.*", x, 0))
    precipitationValues.pprint(num=10000)

    # Parse out just the value without the JSON key
    parsedPrecipitationValues = precipitationValues.map(lambda x: re.sub(r"\"precipitation\":", "", x).split(',')[0])

    # Search for specific IoT data values (assumes jsonLines are split(','))
    speedValues = jsonLines.filter(lambda x: re.findall(r"speed.*", x, 0))
    speedValues.pprint(num=10000)

    # Parse out just the value without the JSON key
    parsedSpeedValues = SpeedValues.map(lambda x: re.sub(r"\"speed\":", "", x))

    # Count how many values were parsed
    countMap = parsedPrecipitationValues.map(lambda x: 1).reduce(add)
    valueCount = countMap.map(lambda x: "Total Count of Msgs: " + unicode(x))
    valueCount.pprint()

    # Sort all the IoT values
    sortedValues = parsedPrecipitationValues.transform(lambda x: x.sortBy(lambda y: y))
    sortedValues.pprint(num=10000)
    sortedValues = parsedSpeedValues.transform(lambda x: x.sortBy(lambda y: y))
    sortedValues.pprint(num=10000)

    # Iterate on each RDD in parsedTempValues DStream to use w/global variables
    parsedPrecipitationValues.foreachRDD(processPrecipitationRDD)

    ssc.start()
    ssc.awaitTermination()
