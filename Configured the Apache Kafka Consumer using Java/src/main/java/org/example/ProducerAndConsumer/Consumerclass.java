package org.example.ProducerAndConsumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class Consumerclass {

    public static void runConsumer() {

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "127.0.0.1:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "console-consumer-65420");
        props.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG,50);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,1000);
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,300000);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,100);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,600000);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());


        // Create the consumer using props.
        final Consumer<String, String> consumer = new KafkaConsumer<>(props);
        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList("quickstart-events"));

        AtomicBoolean recordFetch = new AtomicBoolean(true);
        while (recordFetch.get()) {
            consumer.poll(100).iterator().forEachRemaining(record -> {
                record.value();

                if (record.value().contains("1")){
                    System.out.println(record.value());
                }

            });
            recordFetch.set(false);
            consumer.commitAsync();
        }

        consumer.close();

    }
}
