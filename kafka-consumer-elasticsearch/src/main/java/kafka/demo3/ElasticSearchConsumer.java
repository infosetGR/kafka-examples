package kafka.demo3;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
public class ElasticSearchConsumer {


    public static RestHighLevelClient createClient(){

        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        /////////// IF YOU USE LOCAL ELASTICSEARCH
        //  String hostname = "localhost";
        //  RestClientBuilder builder = RestClient.builder(new HttpHost(hostname,9200,"http"));


        /////////// IF YOU USE BONSAI / HOSTED ELASTICSEARCH
        String hostname =  System.getenv("bonsai_host"); // localhost or bonsai url
        String username = System.getenv("bonsai_username"); // needed only for bonsai
        String password = System.getenv("bonsai_password"); // needed only for bonsai

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                        new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        logger.info( "1");
        return client;
    }


    public static KafkaConsumer<String,String > createConsumer(String topic) {
        //create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kafka-demo-elasticsearch");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); //disable autocommit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe consumer to our topics
        consumer.subscribe(Collections.singleton(topic));
        return consumer;

    }

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client = createClient();





        KafkaConsumer<String,String> consumer = createConsumer("twitter_tweets");


        while (true){
            ConsumerRecords<String,String> records= consumer.poll(Duration.ofMillis(100));

            logger.info("Received " + records.count() + " records");
            if (records.count()>0) {
                BulkRequest bulkRequest = new BulkRequest();

                for (ConsumerRecord record : records) {

                    //twitter feed id
                    try {
                        String id = JsonParser.parseString(record.value().toString()).getAsJsonObject().get("id_str").getAsString();
                        //kafka generic id
                        //String id = record.topic()+"_"+record.partition()+"_"+record.offset();
                        final IndexRequest indexrequest = new IndexRequest("twitter").source(record.value().toString(), XContentType.JSON).id(id);
                        bulkRequest.add(indexrequest);

                    } catch (NullPointerException e) {
                        logger.warn("skipping bad data:" + record.value());
                    }


                }
//                final IndexResponse indexResponse = client.index(indexrequest, RequestOptions.DEFAULT);
//                logger.info( indexResponse.getId());

                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                logger.info("commiting offsets ..");
                consumer.commitSync();
                logger.info("commit done");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

//        client.close();

    }
}
