package kafka.demo1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {
        System.out.println("hello");

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        // create producer
        KafkaProducer<String, String> producer= new KafkaProducer<String, String>(properties);

        for (int i=0; i<10; i++) {

            ProducerRecord<String, String> record =
                    new ProducerRecord("first_topic", "hellp " + i);
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
            });

            producer.flush();
        }
        //flush and close
        producer.close();
    }

}
