package twit

import org.apache.spark._
import org.apache.spark.api.java.function._
import org.apache.spark.streaming._
import org.apache.spark.streaming.api.java._
import org.apache.spark.streaming.twitter._
import twitter4j.GeoLocation
import twitter4j.Status

import twitter4j.auth._
import twitter4j.conf._
import org.apache.spark.streaming.{Seconds,StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.twitter.TwitterUtils

class Tweetstream {
  def main(args:Array[String]) {
        val consumerKey = "XXXXXX";
        val consumerSecret = "xxxxxxxx";
        val accessToken = "xxxxxxxxxx";
        val accessTokenSecret = "xxxxxxxxx";

        //val conf = new SparkConf()
        //conf.setAppName("WordCount")
        
        val conf = new SparkConf().setMaster("local[2]").setAppName("SparkTwitterHelloWorldExample");
        //val jssc = new StreamingContext(conf, new Duration(30000));
        val slideInterval = new Duration(1 * 1000)
        val jobtime = 3 * 1000
        val ssc = new StreamingContext(conf, slideInterval)

        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

        val auth = Some(new OAuthAuthorization(new ConfigurationBuilder().build()))
        val twitterStream = TwitterUtils.createStream(ssc,auth);

        // Without filter: Output text of all tweets
        val hashTagStream = twitterStream.map(_.getText).flatMap(_.split(" ")).filter(_.startsWith("#"))
        
        
        

        // With filter: Only use tweets with geolocation and print location+text.
        /*JavaDStream<Status> tweetsWithLocation = twitterStream.filter(
                new Function<Status, Boolean>() {
                    public Boolean call(Status status){
                        if (status.getGeoLocation() != null) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                }
        );
        JavaDStream<String> statuses = tweetsWithLocation.map(
                new Function<Status, String>() {
                    public String call(Status status) {
                        return status.getGeoLocation().toString() + ": " + status.getText();
                    }
                }
        );*/

        println(hashTagStream)
        ssc.start();
        
        ssc.awaitTerminationOrTimeout(jobtime)
    }
}
