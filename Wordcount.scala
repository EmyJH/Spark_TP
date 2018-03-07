package sparkflair

import org.apache.spark._ ;

object Wordcount {
  def main(args: Array[String])  {
    
    //Create conf object
    val conf = new SparkConf()
    conf.setAppName("WordCount")
 
    //create spark context object
    val sc = new SparkContext(conf)   
    
    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    val bibli = sc.textFile("hdfs:///user/tp-hive/julie/data.csv") //hdfs://ippublique:8080/
    
    //word count
    val counts = bibli.flatMap(line => line.split(" ")) //convert the lines into words using flatMap operation
                     .map(word => (word, 1))            //count the individual words using map 
                     .reduceByKey(_ + _)                // and reduceByKey operation
                     
    counts.foreach(println)
    System.out.println("Total words: " + counts.count());
    counts.saveAsTextFile("hdfs:///user/spark/julie/res_wc1.csv")
    
    //stop the spark context
     //sc.stop
   
  }
}
