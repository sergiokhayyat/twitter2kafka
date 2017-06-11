package com.sergiokhayyat.twitter2kafka;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


/**
 * Based on Twitter's FilterStreamExample
 */
public class Twitter2kafka {

  // Environment variables' names for twitter credentials and search terms
  public static final String TWITTER_CONSUMER_KEY = "TWITTER_CONSUMER_KEY";
  public static final String TWITTER_CONSUMER_SECRET = "TWITTER_CONSUMER_SECRET";
  public static final String TWITTER_ACCESS_TOKEN = "TWITTER_ACCESS_TOKEN";
  public static final String TWITTER_ACCESS_TOKEN_SECRET = "TWITTER_ACCESS_TOKEN_SECRET";
  public static final String SEARCH_TERMS = "SEARCH_TERMS";

  // Kafka config
  public static final String KAFKA_SERVER_PORT = "kafka:9092";
  public static final String KAFKA_TOPIC = "tweets";

  // Private variables for input and output queues
  private BlockingQueue<String> twitterInputQueue;
  private Producer<String, String> kafkaOutputQueue;

  /**
   * Constructor
   */
  public Twitter2kafka(String consumerKey, String consumerSecret, String token, String secret, List<String> twitterTrackTerms) {
    this.twitterInputQueue = getTwitterStream(consumerKey, consumerSecret, token, secret, twitterTrackTerms);
    this.kafkaOutputQueue = getKafkaProducer();
  }

  /**
   * Runner
   */
  public void run() throws InterruptedException {
    // Send tweets to kafka
    long sent = 0;
    while(true) {
      this.sendToKafka(this.twitterInputQueue.take());
      sent++;
      if(sent%1000 == 0) {
        System.err.println(sent + " tweets sent");
      }
    }
  }

  /**
   * Connect to twitter and return a message queue
   */
  private BlockingQueue<String> getTwitterStream(
        String consumerKey, String consumerSecret, String token, String secret,
        List<String> twitterTrackTerms
  ) {
    BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
    StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
    // add some track terms
    endpoint.trackTerms(twitterTrackTerms);

    Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
    // Authentication auth = new BasicAuth(username, password);

    // Create a new BasicClient. By default gzip is enabled.
    Client client = new ClientBuilder()
      .hosts(Constants.STREAM_HOST)
      .endpoint(endpoint)
      .authentication(auth)
      .processor(new StringDelimitedProcessor(queue))
      .build();

    // Establish a connection
    client.connect();

    // Return the queue
    return queue;
  }

  /**
   * Connect to kafka and return a producer queue
   */
  private Producer<String,String> getKafkaProducer() {
    Properties props = new Properties();
    props.put("bootstrap.servers", KAFKA_SERVER_PORT);
    props.put("linger.ms", 10);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    return new KafkaProducer<>(props);
  }

  /**
   * Send something to the kafka producer
   */
  private void sendToKafka(String msg) {
    //System.out.println(msg);
    this.kafkaOutputQueue.send(new ProducerRecord<String, String>(KAFKA_TOPIC, msg));
  }

  /*
   * MAIN
   */
  public static void main(String[] args) {
    // Read twitter credentials from environment variables
    Map<String, String> env = System.getenv();
    String consumerKey = env.get(TWITTER_CONSUMER_KEY);
    String consumerSecret = env.get(TWITTER_CONSUMER_SECRET);
    String token = env.get(TWITTER_ACCESS_TOKEN);
    String secret = env.get(TWITTER_ACCESS_TOKEN_SECRET);
    List<String> twitterTrackTerms = Arrays.asList(env.get(SEARCH_TERMS).split(","));
    try {
      Twitter2kafka t2k = new Twitter2kafka(consumerKey, consumerSecret, token, secret, twitterTrackTerms);
      t2k.run();
    } catch (InterruptedException e) {
      System.err.println(e);
    }
  }
}
