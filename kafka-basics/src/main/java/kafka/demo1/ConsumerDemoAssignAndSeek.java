package kafka.demo1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class ConsumerDemoAssignAndSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignAndSeek.class.getName());

        //create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "My 4th app");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //assign and seek to replay data or fetch specific message

        // assign
        TopicPartition partitionReadFrom = new TopicPartition("first_topic",0);
        consumer.assign(Arrays.asList(partitionReadFrom));
        //seek
        consumer.seek(partitionReadFrom, 15L);

        int numberOfMessagesToRead=5;
        boolean keepOnReading=true;
        int numberOfMessagesReadSoFar=0;
        // poll for new data
        while (true){
          ConsumerRecords<String,String> records= consumer.poll(Duration.ofMillis(100));

          for (ConsumerRecord record : records){
              logger.info("Key:"+record.key()+", Value:"+record.value());
              logger.info("Partition:"+record.partition()+", Value:"+record.offset());
              if (numberOfMessagesReadSoFar>= numberOfMessagesToRead){
                  keepOnReading=false;
                  break;
              }
          }
        }

    }
}
