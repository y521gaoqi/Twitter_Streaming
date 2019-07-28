




import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.twitter._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.DStream
import twitter4j.Status
import java.util.{HashMap, Properties}

import com.google.common.io.Files
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.SentimentAnnotatedTree
import edu.stanford.nlp.util.CoreMap

object Twitter {

  def main(args: Array[String]): Unit= {

    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }
    val consumerKey="9wp067AoV25WtNinbXmWo0jEc"
    val consumerSecret="JnC3wby0JpnkjX4t7HinM3rdsUDFhCGUa1OEQGwJomAfvJTY9E"
    val accessToken="3143040895-KXIOOLyg5Eqo3YRcQUpk65ClzjuruYxfrK9d75n"
    val accessTokenSecret="qWhjRm3Q11qotTHYG65m6JsNQTgd1HlNnHQlwCYtTlAMq"

    System.setProperty("twitter4j.oauth.consumerKey",consumerKey )
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    var z = new Array[String](1)
    z(0) = args(0);
    val topic = args(1)
    val key_word = z
    val broker = "localhost:9092"
    val conf = new SparkConf().
      setAppName("spark-twitter-stream-example").
      setMaster(sys.env.get("spark.master").getOrElse("local[*]"))
    val sparkContext = new SparkContext(conf)
    sparkContext.setLogLevel("ERROR")
    val streamingContext = new StreamingContext(sparkContext, Seconds(5))
    val t =
      TwitterUtils.createStream(streamingContext, None, key_word)

    val tweets_text =
    t.filter(x => x.getLang == "en").map(_.getText).map(x=>SentimentAnalyzer.mainSentiment(x))
    tweets_text.foreachRDD( rdd => {

      rdd.foreachPartition( partition => {
        val p = new HashMap[String, Object]()
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer")
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer")
        val producer = new KafkaProducer[String, String](p)
        partition.foreach( record => {
          val data = record.toString
          val message = new ProducerRecord[String, String](topic, null, data)
          producer.send(message)
        } )
        producer.close()
      })

    })
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}



