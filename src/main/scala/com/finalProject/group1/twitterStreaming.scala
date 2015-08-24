import com.finalProject.group1.StreamingLogger
import com.finalProject.group1.SentimentAnalysisUtils._
import com.typesafe.config.ConfigFactory
import java.io.File
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.rdd.EsSpark
import org.apache.spark.SparkContext._
import org.json4s.JsonDSL._
import java.text.SimpleDateFormat
import org.joda.time.{DateTime, Days}
import scala.util.Try
object twitterStreaming {
  def main(args: Array[String]) {

    // Assigning logger level to remove garbage
    StreamingLogger.setStreamingLogLevels()

    // Reading the twitter configurations from conf file
    val parsedConfig = ConfigFactory.parseFile(new File("C:/Users/Bellamkonda/Documents/Twitter/config/TwitterScreamer.conf"))
    //val parsedConfig = ConfigFactory.parseFile(new File("/home/ec2-user/config/TwitterScreamer.conf"))
    // Loading config file
    val conf = ConfigFactory.load(parsedConfig)

    // Getting the credential using OAUTH
    val credential = conf.getConfig("oauthcredentials")
    val consumer_key = credential.getString("consumerKey")
    val consumer_secret = credential.getString("consumerSecret")
    val access_token = credential.getString("accessToken")
    val access_token_secret = credential.getString("accessTokenSecret")

    // Accept twitter stream
    val values = if (args.length == 0) List() else args.toList

    // setting key & values
    System.setProperty("twitter4j.oauth.consumerKey", consumer_key)
    System.setProperty("twitter4j.oauth.consumerSecret", consumer_secret)
    System.setProperty("twitter4j.oauth.accessToken", access_token)
    System.setProperty("twitter4j.oauth.accessTokenSecret", access_token_secret)

    // Initialize a SparkConf
    val sparkConf = new SparkConf().setAppName("TwitterSpam_sentiment").setMaster("local[*]")
    //  Indexing in Elasticsearch server
    sparkConf.set("es.resource","sparkstream/tweets")
    sparkConf.set("es.index.auto.create", "false")

    //Defining elasticsearch server
    sparkConf.set("es.nodes", "52.6.37.57")
    sparkConf.set("es.port", "9200")

    // Spark stream batch interval
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val stream = TwitterUtils.createStream(ssc, None, values)

    // Processing each tweet in a batch
    val tweetMap = stream.map(status => {
      val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZ")
      def checkObj (x : Any) : String  = {
        x match {
          case i:String => i
          case _ => ""
        }
      }
      // Creating a JSON
      val tweetMap =
        ("UserId" -> status.getUser.getId) ~
          ("UserDescription" -> status.getUser.getDescription) ~
          ("UserScreenName" -> status.getUser.getScreenName) ~
          ("UserFriendsCount" -> status.getUser.getFriendsCount) ~
          ("UserFavouritesCount" -> status.getUser.getFavouritesCount) ~
          ("UserFollowersCount" -> status.getUser.getFollowersCount) ~
          ("UserFollowersRatio" -> status.getUser.getFollowersCount.toFloat / status.getUser.getFriendsCount.toFloat) ~
          ("Retweet" -> status.getRetweetCount) ~
          ("UserLang" -> status.getUser.getLang) ~
          ("Geo_location" -> Option(status.getGeoLocation).map(geo => { s"${geo.getLatitude},${geo.getLongitude}" })) ~
          ("UserLocation" -> status.getUser.getLocation) ~
          ("UserVerification" -> status.getUser.isVerified) ~
          ("UserName" -> status.getUser.getName) ~
          ("UserStatusCount" -> status.getUser.getStatusesCount) ~
          ("UserCreated" -> formatter.format(status.getUser.getCreatedAt.getTime)) ~
          ("Text" -> status.getText) ~
          ("TextLength" -> status.getText.length) ~
          ("HashTags" -> status.getText.split(" ").filter(_.startsWith("#")).mkString(" ")) ~
          ("StatusCreatedAt" -> formatter.format(status.getCreatedAt.getTime))~
          ("Sentiment" -> detectSentiment(status.getText).toString)

      // Spam function
      def spamDetection(tweet: Map[String, Any]): Boolean = {
        {
          Days.daysBetween(new DateTime(formatter.parse(tweet.get("UserCreated").mkString).getTime),
            DateTime.now).getDays > 1
        } & {
          tweet.get("UserStatusCount").mkString.toInt > 50
        } & {
          tweet.get("UserFollowersRatio").mkString.toFloat > 0.01
        } & {
          tweet.get("UserDescription").mkString.length > 20
        } & {
          tweet.get("Text").mkString.split(" ").filter(_.startsWith("#")).length < 5
        } & {
          tweet.get("TextLength").mkString.toInt > 20
        } & {
          val filters = List("rt and follow", "rt & follow", "rt+follow", "follow and rt", "follow & rt", "follow+rt")
          !filters.exists(tweet.get("Text").mkString.toLowerCase.contains)
        }
      }
      spamDetection(tweetMap.values) match {
        case true => tweetMap.values.+("Spam" -> false)
        case _ => tweetMap.values.+("Spam" -> true)
      }

    })
    tweetMap.map(s => List("Tweet extracted successfully")).print
    //saving in to elastic search
    tweetMap.foreachRDD { tweets => EsSpark.saveToEs(tweets, "sparkstream/tweets", Map("es.mapping.timestamp" -> "StatusCreatedAt")) }
    ssc.start
    //ssc.checkpoint("C:/Users/Bellamkonda/Desktop/Checkpoints")
    ssc.awaitTermination
  }
}
