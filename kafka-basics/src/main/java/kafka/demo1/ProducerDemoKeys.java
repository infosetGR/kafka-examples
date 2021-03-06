package kafka.demo1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        // create producer
        KafkaProducer<String, String> producer= new KafkaProducer<String, String>(properties);

        for (int i=0; i<10; i++) {

            String topic = "first_topic";
            String value = "hello world " + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            ProducerRecord<String, String> record =
                    new ProducerRecord(topic,key,value );
            logger.info("Key: " + key); // log key
            //send data -async
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes on success sent or exception
                    if (e == null) {
                        logger.info("Receive new metadata \n Topic " + recordMetadata.topic() + "\n Partition " + recordMetadata.partition() + "\n Offsets " + recordMetadata.offset() + "\n Timestamp " + recordMetadata.timestamp());

                    } else {
                        logger.error("error", e);

                    }
                }
            }).get(); // making it synchronous not for production

            producer.flush();
        }
        //flush and close
        producer.close();
    }

}
