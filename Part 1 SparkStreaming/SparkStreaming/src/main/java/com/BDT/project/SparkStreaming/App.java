package com.BDT.project.SparkStreaming;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
 
import scala.Tuple2;
import twitter4j.Status;

/**
 * Perform sentiment analysis on a stream tweets
 *
 */
public class App 
{
	

	public static Long TIME_BATCH_ANALYSIS = (long) 40000;
	public static String FILE_ANALYSED = "/home/cloudera/cs523/TweetsRecorded"; // directory to record tweets used during our analysis
	public static String CATALOG_SENTIMENT = "data/CATALOG_SENTIMENT.txt";
	public static List<String> KEY_WORD = Arrays.asList("sport", "football", "win"); //list of key words subject of our analysis

	public static boolean stringContainListElt(String elt, List<String> listElt ) {
        for(String word : listElt){
            if(StringUtils.containsIgnoreCase(elt,word)) return true;
        };
        return false;
    }
	
  public static void main(String[] args) {
    if (args.length < 4) {
      System.err.println("Usage: JavaTwitterHashTagJoinSentiments <consumer key>" +
        " <consumer secret> <access token> <access token secret> [<filters>]");
      System.exit(1);
    }
    TwiterHbaseTable.reinitDB();
    
    //StreamingExamples.setStreamingLogLevels();
    // Set logging level if log4j not configured (override by adding log4j.properties to classpath)
    if (!Logger.getRootLogger().getAllAppenders().hasMoreElements()) {
      Logger.getRootLogger().setLevel(Level.WARN);
    }

    String consumerKey = args[0];
    String consumerSecret = args[1];
    String accessToken = args[2];
    String accessTokenSecret = args[3];
    String[] filters = Arrays.copyOfRange(args, 4, args.length);

    // Set the system properties so that Twitter4j library used by Twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
    System.setProperty("twitter4j.oauth.accessToken", accessToken);
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

    SparkConf sparkConf = new SparkConf().setAppName("SparkStreaming");

    // check Spark configuration for master URL, set it to local if not configured
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]");
    }
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(TIME_BATCH_ANALYSIS));
    JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jssc, filters);
    
    
    JavaDStream<String> words = stream
    		.map(tweet->{
    			String text = tweet.getText().replace('#', ' ')
    							.replace('.', ' ').replace(';', ' ')
    							.replace(':', ' ').replace('!', ' ').replace('?', ' ');
    			System.out.println(text);
    			return text;
    		})
    		.filter((tweet) -> stringContainListElt(tweet, KEY_WORD))
    		.flatMap(line->{
    			System.out.println(line);
    			return Arrays.asList(line.split(" ")).iterator();
    			}
  );
    // Save in textfile tweets used for our analysis
    words.foreachRDD(a->{
    	a.saveAsTextFile(FILE_ANALYSED);
    });
    
    // Read in the word-sentiment list and create a static RDD from it
    String wordSentimentFilePath = CATALOG_SENTIMENT;
    final JavaPairRDD<String, Double> wordSentiments = jssc.sparkContext()
      .textFile(wordSentimentFilePath)
      .mapToPair(line->{
    	  String[] columns = line.split("\t");
          return new Tuple2<>(columns[0], Double.parseDouble(columns[1]));
      });

    JavaPairDStream<String, Integer> hashTagCount = words.mapToPair(
    													s->new Tuple2<>(s, 1));

    JavaPairDStream<String, Integer> hashTagTotals = hashTagCount.reduceByKeyAndWindow(
    														(a,b)->a+b, new Duration(TIME_BATCH_ANALYSIS));

    // Determine the sentiment values of tweets by joining the streaming RDD
    // with the static RDD inside the transform() method and then multiplying
    // the frequency of the hash tag by its sentiment value
    JavaPairDStream<String, Tuple2<Double, Integer>> joinedTuples =
      hashTagTotals.transformToPair(
    		  topicCount->wordSentiments.join(topicCount)
    		 );

    JavaPairDStream<String, Integer> topicHappiness = joinedTuples.mapToPair(
    		topicAndTuplePair->{
	    			Tuple2<Double, Integer> happinessAndCount = topicAndTuplePair._2();
	    			Double grad = Math.pow(happinessAndCount._1(), happinessAndCount._2());
	    			Integer grade = 0;
	    			if(grad > 0.0) grade = 1;
	    			if(grad < 0.0) grade = -1;
	    			if(grad == 0) grade = 0; 
		            return new Tuple2<>(topicAndTuplePair._1(),  grade);
	    		}
    		);

    JavaPairDStream<String, Integer> happinessTopicPairs = topicHappiness
    		.filter(a-> a._2>0);
	JavaPairDStream<String, Integer> unHappinessTopicPairs = topicHappiness
    		.filter(a-> a._2<0);
    		

    // Print and record outcomes of our analysis
    happinessTopicPairs.foreachRDD(happinessTopicPair-> {
    	  
    	Integer happiestTipics =  happinessTopicPair.values()
    	  .collect().stream().reduce((a,b)->a+b).orElse(Integer.valueOf("0"));
    	String record = "############Happy of :"+KEY_WORD +" : "+happiestTipics;
    	System.out.println(record);
    	
    		
    		TwiterHbaseTable.saveTweet(KEY_WORD.toString(),happiestTipics);
      }
    );
    unHappinessTopicPairs.foreachRDD(happinessTopicPair-> {
  	  
    	Integer unHappiestTipics =  happinessTopicPair.values()
    	  .collect().stream().reduce((a,b)->a+b).orElse(Integer.valueOf("0"));
    	String record = "UnHappy of :"+KEY_WORD +" : "+unHappiestTipics;
    	System.out.println(record);
    	
    		TwiterHbaseTable.saveTweet(KEY_WORD.toString(),unHappiestTipics);
		
    });
    
   
    jssc.start();
    try {
      jssc.awaitTermination();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
