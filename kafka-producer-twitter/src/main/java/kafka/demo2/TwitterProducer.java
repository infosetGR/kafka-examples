package kafka.demo2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


public class TwitterProducer {


    String consumerKey=System.getenv("consumerKey");
    String consumerSecret=System.getenv("consumerSecret");
    String token=System.getenv("token");
    String secret=System.getenv("secret");
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    public static void main(String[] args) {
        new TwitterProducer().run();
    }


    public TwitterProducer(){}

    public void run() {
        // create twitter client
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(100);
//        BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);
        List<String> terms = Lists.newArrayList("google");
        Client client = createTwitterClient(msgQueue,eventQueue,terms);
        client.connect();

        //kafka producer
        KafkaProducer<String, String> producer= createKafkaProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("stopping application");
            logger.info("stopping client from twitter");
            client.stop();
            logger.info("closing producer");
            producer.close();
            logger.info("Done");
        }));

        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            Event evt = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
                System.out.println("poll");
                System.out.println(msg);
              //  evt = eventQueue.poll(5,TimeUnit.SECONDS);
//                System.out.println("event");
                //System.out.println(evt..getMessage());

            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg!=null){
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e !=null){
                            logger.error("Error occured",e);
                        }
                    }
                });
            }

        }

        logger.info("-----End-----");
    }


    public Client createTwitterClient(BlockingQueue<String> msgQueue, BlockingQueue<Event> eventQueue, List<String> terms) {

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<Long> followings = Lists.newArrayList(1234L, 566788L);

        hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
               // .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();

        return hosebirdClient;

    }

    public KafkaProducer<String, String> createKafkaProducer(){
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5"); //KAFKA 2.0 >1.1 else 1

        //high throughput
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024)); //32 kb

        // create producer
        KafkaProducer<String, String> producer= new KafkaProducer<String, String>(properties);

       return producer;
    }
}
