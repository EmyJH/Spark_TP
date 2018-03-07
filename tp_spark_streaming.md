# TP spark streaming
```Linux
sbin/start-master.sh **
 sbin/start-slave.sh spark://
```

```scala

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3


// Create a local StreamingContext with two working thread and batch interval of 1 second.
// The master requires 2 cores to prevent a starvation scenario.

//val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount").set("spark.driver.allowMultipleContexts" , "true")
val ssc = new StreamingContext(sc, Seconds(1))

// Create a DStream that will connect to hostname:port, like localhost:9999
val lines = ssc.socketTextStream("ip-172-31-18-227.eu-west-1.compute.internal", 9999)

// Split each line into words
val words = lines.flatMap(_.split(" "))
val pairs = words.map(word => (word, 1))
val wordCounts = pairs.reduceByKey(_ + _)

// Print the first ten elements of each RDD generated in this DStream to the console
println("ok")
wordCounts.print()
ssc.start()
ssc.awaitTermination()

```
me